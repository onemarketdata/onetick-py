import os
import re
import warnings
import shutil
from datetime import datetime
from typing import List

from locator_parser.io import FileReader, FileWriter, PrintWriter
from locator_parser.actions import Add, GetAll, Delete
from locator_parser.common import apply_actions
from locator_parser import locator as _locator
from locator_parser import acl as _acl
from abc import ABC, abstractmethod
from onetick.py.otq import otli
from . import utils
from . import license as _license
from . import db as _db
from . import servers as _servers
from . import configuration
from .db._inspection import databases as _databases


class EntityOperationFailed(Exception):
    """
    Raise when operation with an entity for a config
    in this module failed
    """


class MultipleSessionsException(Exception):
    """
    Raises when user tries to initiate a new one Session
    when another one is already used
    """


def _check_entities(entities):
    if not entities:
        raise ValueError("At least one argument in parameter 'entities' is expected.")


def _apply_to_entities(cfg, operations):
    """
    Function generalizes operations for locators and ACLs:
    tries to apply ``operations``, and does rollback in case
    OneTick config is invalid.
    """
    if not operations:
        return

    for entities, func, roll_back_func in operations:
        _check_entities(entities)
        result = func(entities)
        if not result:
            entity_name = entities[0].__class__.__name__
            entities = list(map(str, entities))
            raise EntityOperationFailed(
                f'Operation {func.__name__} for {entity_name}s {entities}'
                f' for {cfg.__class__.__name__} "{cfg.path}" has failed'
            )
    try:
        cfg.reload()
    except Exception:
        for entities, func, roll_back_func in reversed(operations):
            roll_back_func(entities)
        raise


class _FileHandler(ABC):
    def __init__(self, file_h=None, clean_up=utils.default, copied=True, session_ref=None):
        self._file = file_h
        self._clean_up = clean_up
        # flag to understand whether we work with externally passed files;
        # it is set and affects logic, when copy=False
        self._copied = copied
        self._session_ref = session_ref

    @property
    def path(self):
        return self._file.path

    @property
    def file(self):
        return self._file

    def copy(self, clean_up=utils.default, copy=True, session_ref=None):
        return self.__class__(self.path, clean_up=clean_up, copy=copy, session_ref=session_ref)  # pylint: disable=E1123

    @abstractmethod
    def cleanup(self):
        pass

    @staticmethod
    def _db_in_dbs_case_insensitive(db_id: str, databases: List[str]):
        for db_name in databases:
            if db_id.upper() == db_name.upper():
                return True
        return False


class _CommonBuilder(ABC):
    def __init__(self, src=None, clean_up=utils.default, copy=True, session_ref=None):
        self.src = src
        self.clean_up = clean_up
        self.copy = copy
        self.session_ref = session_ref

    @abstractmethod
    def build(self):
        pass


class ACL(_FileHandler):
    class User(str):
        """
        Subclass represents an ACL user
        """

        pass

    def __init__(self, path=None, clean_up=utils.default, copy=True, session_ref=None):
        """
        Class representing OneTick database access list file.
        ACL is the file that describes the list of the users
        that are allowed to access the database and what permissions do they have.

        Parameters
        ----------
        path: str
            A path to custom acl file. Default is `None`, that means to generate a temporary acl file.
        clean_up: bool
            If `True`, then temporary acl file will be removed when ACL object will be destroyed. It is
            helpful for debug purpose.

            By default,
            :py:attr:`otp.config.clean_up_tmp_files<onetick.py.configuration.Config.clean_up_tmp_files>` is used.
        copy: bool
            If `True`, then the passed custom acl file by the ``path`` parameter will be copied first before
            usage. It might be used when you want to work with a custom acl file, but don't want to change
            the original file; in that case a custom acl file will be copied into a temporary file and
            every request for modification will be executed for that temporary file. Default is `True`.
        """

        copied = True

        # TODO: implement this logic later
        # if copy is None and path is not None:
        #    # if copy rule is not specified, but path is specified
        #    # then we set copy to True with safety goal, otherwise
        #    # we would might change a permanent file without user
        #    # acknowledgment
        #    copy = True
        #    raise Warning("You set the ACL file, but have not specify a copy rule. "
        #                  "We copy it with safety goal, and it means you will work "
        #                  "with copied file instead of passed. If you want to work "
        #                  "with passed file directly, then you could set the 'copy' "
        #                  "parameter to True.")

        # if path is set, then copy file, we should not work directly
        # with externally passed files
        if copy:
            if path:
                file_h = utils.TmpFile(suffix=".acl", clean_up=clean_up)
                shutil.copyfile(path, file_h.path)
            else:
                file_h = utils.tmp_acl(clean_up=clean_up)
        else:
            if path:
                file_h = utils.PermanentFile(path)
                copied = False
            else:
                file_h = utils.tmp_acl(clean_up=clean_up)

        assert file_h is not None

        super().__init__(file_h, clean_up=clean_up, copied=copied, session_ref=session_ref)

        self._added_dbs = []

    def cleanup(self):
        self._remove_db(self._added_dbs)
        self._added_dbs = []
        self.reload()

    def _apply_actions(self, actions, print_writer=False):
        writer = PrintWriter() if print_writer else FileWriter(self.path)
        flush = False if print_writer else True
        return apply_actions(_acl.parse_acl, FileReader(self.path), writer, actions, flush=flush)

    def _add_db(self, dbs):
        actions = []
        for db in dbs:
            actions.append(Add(_acl.DB(id=db.id, read_access="true")))

            permissions = {}
            if db._write:
                permissions["write_access"] = "true"
            if hasattr(db, "_destroy_access") and db._destroy_access:
                permissions["destroy_access"] = "true"
            if db._minimum_start_date:
                permissions['minimum_start_date'] = db._minimum_start_date.strftime('%Y%m%d')
            if db._maximum_end_date:
                permissions['maximum_end_date'] = db._maximum_end_date.strftime('%Y%m%d')

            if permissions:
                action = Add(_acl.Allow(role="Admin", **permissions))
                action.add_where(_acl.DB, id=db.id)
                actions.append(action)

        return self._apply_actions(actions)

    def _remove_db(self, dbs):
        actions = []
        for db in dbs:
            action = Delete()
            action.add_where(_acl.DB, id=db.id)
            actions.append(action)

        return self._apply_actions(actions)

    def _add_user(self, users):
        actions = []
        for user in users:
            action = Add(_acl.User(name=user))
            action.add_where(_acl.Role, name="Admin")
            actions.append(action)

        return self._apply_actions(actions)

    def _remove_user(self, users):
        actions = []
        for user in users:
            action = Delete()
            action.add_where(_acl.Role, name="Admin")
            action.add_where(_acl.User, name=user)
            actions.append(action)

        return self._apply_actions(actions)

    def add(self, *entities):
        """
        Add entities to the ACL and reload it.
        If it fails, then tries to roll back to the original state.

        Parameters
        ----------
        entities: DB or ACL.User

        Raises
        ------
        TypeError, EntityOperationFailed
        """
        _check_entities(entities)

        dbs = []
        users = []
        for entity in entities:
            if isinstance(entity, _db.DB):
                if self._db_in_dbs_case_insensitive(entity.id, self.databases):
                    if '//' not in entity.name:
                        warnings.warn(f"Database '{entity.id}' is already added to the ACL"
                                      " and will not be rewritten with this command."
                                      f" Notice that databases' names are case insensitive.",
                                      stacklevel=2)
                    continue
                dbs.append(entity)
            elif isinstance(entity, ACL.User):
                users.append(entity)
            else:
                raise TypeError(f'Entity of type "{type(entity)}" is not supported')

        operations = []
        if dbs:
            operations.append((dbs, self._add_db, self._remove_db))
        if users:
            operations.append((users, self._add_user, self._remove_user))

        _apply_to_entities(self, operations)

        self._added_dbs.extend(dbs)

    def remove(self, *entities):
        """
        Remove entities from the ACL and reload it.
        If it fails, then tries to roll back to the original state.

        Parameters
        ----------
        entities: DB or ACL.User

        Raises
        ------
        ValueError, TypeError, EntityOperationFailed
        """
        _check_entities(entities)

        dbs = []
        users = []
        for entity in entities:
            if isinstance(entity, _db.DB):
                if entity not in self._added_dbs:
                    raise ValueError(f'DB "{entity}" was not added')
                dbs.append(entity)
            elif isinstance(entity, ACL.User):
                users.append(entity)
            else:
                raise TypeError(f'Entity of type "{type(entity)}" is not supported')

        operations = []
        if dbs:
            operations.append((dbs, self._remove_db, self._add_db))
        if users:
            operations.append((users, self._remove_user, self._add_user))

        _apply_to_entities(self, operations)

        for db in dbs:
            self._added_dbs.remove(db)

    def reload(self, db=None):
        if self._session_ref is not None:
            return utils.reload_config(db, config_type='ACCESS_LIST')
        return None

    def _read_dbs(self):
        get_db = GetAll()
        get_db.add_where(_acl.DB)
        self._apply_actions([get_db], print_writer=True)
        return list(map(lambda x: x.id, get_db.result))

    def _dbs(self):
        action = GetAll()
        action.add_where(_acl.DB)
        self._apply_actions([action], print_writer=True)
        return list(map(lambda x: x.id, action.result))

    def _users(self):
        action = GetAll()
        action.add_where(_acl.Role, name="Admin")
        action.add_where(_acl.User)
        self._apply_actions([action], print_writer=True)
        return list(map(lambda x: x.name, action.result))

    @property
    def databases(self):
        return self._dbs()

    @property
    def users(self):
        return self._users()


class ACLBuilder(_CommonBuilder):
    def build(self):
        params = {"clean_up": self.clean_up, "copy": self.copy, "session_ref": self.session_ref}

        if isinstance(self.src, str):
            return ACL(self.src, **params)
        elif isinstance(self.src, utils.File):
            return ACL(self.src.path, **params)
        elif isinstance(self.src, ACL):
            return self.src.copy(**params)
        elif self.src is None:
            return ACL(**params)

        raise ValueError(f'It is not allowed to build ACL from the object of type "{type(self.src)}"')


class Locator(_FileHandler):
    def __init__(self, path=None, clean_up=utils.default, copy=True, empty=False, session_ref=None):
        """
        Class representing OneTick database locator.
        Locator is the file that describes database name, location and other options.

        Parameters
        ----------
        path: str
            A path to custom locator file. Default is `None`, that means to generate a temporary locator.
        clean_up: bool
            If True, then temporary locator will be removed when Locator object will be destroyed. It is
            helpful for debug purpose.

            By default,
            :py:attr:`otp.config.clean_up_tmp_files<onetick.py.configuration.Config.clean_up_tmp_files>` is used.
        copy: bool
            If `True`, then the passed custom locator by the ``path`` parameter will be copied firstly before
            usage. It might be used when you want to work with a custom locator, but don't want to change
            the original file; in that case a custom locator will be copied into a temporary locator and
            every request for modification will be executed for that temporary locator. Default is `True`.
        empty: bool
            If `True`, then a temporary locator will have no databases, otherwise it will have default
            otp.config.default_db and COMMON databases. Default is `False`.
        """
        copied = True

        # if path is set, then copy file, we should not work directly
        # with externally passed files
        if copy:
            if path:
                file_h = utils.TmpFile(".locator", clean_up=clean_up)
                shutil.copyfile(path, file_h)
            else:
                file_h = utils.tmp_locator(clean_up=clean_up, empty=empty)

        else:
            if path:
                file_h = utils.PermanentFile(path)
                copied = False
            else:
                file_h = utils.tmp_locator(clean_up=clean_up, empty=empty)

        assert file_h is not None

        super().__init__(file_h, clean_up=clean_up, copied=copied, session_ref=session_ref)

        self._added_dbs = []
        self._added_ts = []

    def cleanup(self):
        self._remove_db(self._added_dbs)
        self._remove_ts(str(server) for server in self._added_ts)
        self._added_dbs = []
        self._added_ts = []
        self.reload()

    @property
    def databases(self):
        return self._dbs()

    @property
    def tick_servers(self):
        return self._ts()

    def reload(self, db_=None):
        if self._session_ref is not None:
            return utils.reload_config(db_, config_type='LOCATOR')
        return None

    def _apply_actions(self, actions, print_writer=False):
        writer = PrintWriter() if print_writer else FileWriter(self.path)
        flush = False if print_writer else True
        return apply_actions(_locator.parse_locator, FileReader(self.path), writer, actions, flush=flush)

    def _dbs(self):
        action = GetAll()
        action.add_where(_locator.DB)
        self._apply_actions([action], print_writer=True)
        return list(map(lambda x: x.id, action.result))

    def _ts(self):
        get_ts = GetAll()
        get_ts.add_where(_locator.TickServers)
        get_ts.add_where(_locator.ServerLocation)
        self._apply_actions([get_ts], print_writer=True)
        return [location.location for location in get_ts.result]

    def _add_db(self, dbs):
        actions = []
        for db in dbs:
            actions.append(Add(_locator.DB(id=db.id, **db.properties)))
            for location in db.locations:
                action = Add(_locator.Location(**location))
                action.add_where(_locator.DB, id=db.id)
                actions.append(action)

            for raw_db in db.raw_data:
                common = {k: v for k, v in raw_db.items() if k not in {'id', 'locations'}}
                action = Add(_locator.RawDB(id=raw_db['id'], **common))
                action.add_where(_locator.DB, id=db.id)
                actions.append(action)
                for location in raw_db['locations']:
                    action = Add(_locator.Location(**location))
                    action.add_where(_locator.DB, id=db.id)
                    action.add_where(_locator.RawDB, id=raw_db['id'])
                    actions.append(action)

            if db.feed:
                options = {k: v for k, v in db.feed.items() if k != 'type'}
                action = Add(_locator.Feed(type=db.feed['type']))
                action.add_where(_locator.DB, id=db.id)
                actions.append(action)
                action = Add(_locator.FeedOptions(**options))
                action.add_where(_locator.DB, id=db.id)
                action.add_where(_locator.Feed, type=db.feed['type'])
                actions.append(action)

        return self._apply_actions(actions)

    def _remove_db(self, dbs):
        actions = []
        for db in dbs:
            action = Delete()
            action.add_where(_locator.DB, id=db.id)
            actions.append(action)

        return self._apply_actions(actions)

    def _add_ts(self, servers):
        """
        Add servers to locator file (without reloading)

        Parameters
        ----------
        servers: RemoteTS
            Servers to be added to locator.
        """
        actions = []
        for server in servers:
            if server.cep:
                actions.append(Add(_locator.CEPServerLocation(location=str(server))))
            else:
                actions.append(Add(_locator.ServerLocation(location=str(server))))

        return self._apply_actions(actions)

    def _remove_ts(self, servers):
        """
        Remove servers from locator file (without reloading)

        Parameters
        ----------
        servers: RemoteTS
            Servers to remove from locator
        """
        actions = []
        for server in servers:
            action = Delete()
            action.add_where(_locator.ServerLocation, location=str(server))
            actions.append(action)

        return self._apply_actions(actions)

    def _add_locator(self, locators):
        """
        Add references to locators

        Parameters
        ----------
        locators: Locator
        """
        actions = []
        for locator in locators:
            actions.append(Add(_locator.Include(path=locator.path)))

        return self._apply_actions(actions)

    def _remove_locator(self, locators):
        """
        Remove references for locators

        Parameters
        ----------
        locators: Locator
        """
        actions = []
        for locator in locators:
            action = Delete()
            action.add_where(_locator.Include, path=locator.path)
            actions.append(action)

        return self._apply_actions(actions)

    def add(self, *entities):
        """
        Add entities to the locator and reload it.
        If it fails, then tries to roll back to the original state.

        Parameters
        ----------
        entities: DB, RemoteTS or Locator

        Raises
        ------
        TypeError, EntityOperationFailed
        """
        _check_entities(entities)

        dbs = []
        servers = []
        locators = []
        for entity in entities:
            if isinstance(entity, _db.db._DB):
                if self._db_in_dbs_case_insensitive(entity.id, self.databases):
                    if '//' not in entity.name:
                        warnings.warn(f"Database '{entity.id}' is already added to the Locator"
                                      " and will not be rewritten with this command."
                                      f" Notice that databases' names are case insensitive.", stacklevel=2)
                    continue
                dbs.append(entity)
            elif isinstance(entity, _servers.RemoteTS):
                servers.append(entity)
            elif isinstance(entity, Locator):
                locators.append(entity)
            else:
                raise TypeError(f'Entity of type "{type(entity)}" is not supported')

        operations = []
        if dbs:
            operations.append((dbs, self._add_db, self._remove_db))
        if servers:
            operations.append((servers, self._add_ts, self._remove_ts))
        if locators:
            operations.append((locators, self._add_locator, self._remove_locator))

        _apply_to_entities(self, operations)

        self._added_dbs.extend(dbs)
        self._added_ts.extend(servers)

    def remove(self, *entities):
        """
        Remove entities from the locator and reload it.
        If it fails, then tries to roll back to the original state.

        Raises
        ------
        ValueError, TypeError, EntityOperationFailed
        """
        _check_entities(entities)

        dbs = []
        servers = []
        locators = []
        for entity in entities:
            if isinstance(entity, _db.db._DB):
                if entity not in self._added_dbs:
                    raise ValueError(f'DB "{entity}" was not added')
                dbs.append(entity)
            elif isinstance(entity, _servers.RemoteTS):
                if entity not in self._added_ts:
                    raise ValueError(f'Tick server "{entity}" was not added')
                servers.append(entity)
            elif isinstance(entity, Locator):
                locators.append(entity)
            else:
                raise TypeError(f'Entity of type "{type(entity)}" is not supported')

        operations = []
        if dbs:
            operations.append((dbs, self._remove_db, self._add_db))
        if servers:
            operations.append((servers, self._remove_ts, self._add_ts))
        if locators:
            operations.append((locators, self._remove_locator, self._add_locator))

        _apply_to_entities(self, operations)

        for db in dbs:
            self._added_dbs.remove(db)
        for server in servers:
            self._added_ts.remove(server)

    def __contains__(self, item):
        if str(item) in self.databases:
            return True
        return False


class LocatorBuilder(_CommonBuilder):
    def build(self):
        params = {"clean_up": self.clean_up, "copy": self.copy, "session_ref": self.session_ref}

        if isinstance(self.src, str):
            return Locator(self.src, **params)
        elif isinstance(self.src, utils.File):
            return Locator(self.src.path, **params)
        elif isinstance(self.src, Locator):
            return self.src.copy(**params)
        elif isinstance(self.src, _servers.RemoteTS):
            locator = Locator(empty=True, **params)
            locator.add(self.src)
            return locator
        elif self.src is None:
            return Locator(**params)

        raise ValueError(f'It is not allowed to build Locator from the object of type "{type(self.src)}"')


class Config(_FileHandler):

    _CONFIG_VARIABLES_PASSED_VIA_THEIR_OWN_PARAMETER = {
        'ACCESS_CONTROL_FILE': 'acl',
        'DB_LOCATOR.DEFAULT': 'locator',
        'OTQ_FILE_PATH': 'otq_path',
        'CSV_FILE_PATH': 'csv_path',
        'LICENSE_REPOSITORY_DIR': 'license',
        'ONE_TICK_LICENSE_FILE': 'license',
    }

    def __init__(
        self,
        config=None,
        locator=None,
        acl=None,
        otq_path=None,
        csv_path=None,
        clean_up=utils.default,
        copy=True,
        session_ref=None,
        license=None,
        variables=None,
    ):
        """
        Parameters
        ----------
        config: path or Config
            Allows to specify a custom config. None is to use temporary generated config. Default is None.
        locator: Locator
            Allows to specify a custom locator file. None is to use temporary generated locator. Default is None.
        acl: ACL
            Allows to specify a custom acl file. None is to use temporary generated acl. Default is None.
        otq_path: list of paths to lookup queries
            OTQ_PATH parameter in the OneTick config file. Default is None, that is equal to the empty list.
        csv_path: list of paths to lookup csv files
            CSV_PATH parameter in the OneTick config file. Default is None, that is equal to the empty list.
        clean_up: bool
            If True, then temporary config file will be removed when the Config instance will be destroyed.
            It is helpful for debug purpose.

            By default,
            :py:attr:`otp.config.clean_up_tmp_files<onetick.py.configuration.Config.clean_up_tmp_files>` is used.
        copy: bool
            If True, then the passed custom config file will be copied firstly before any usage with it.
            It might be used when you want to work with a custom config file, but don't want to change to
            change the original file; in that case a custom config will be copied into a temporary config
            file and every request for modification will be executed for that temporary config. Default
            is True.
        license: instance from the onetick.py.license module
            License to use. If it is not set, then onetick.py.license.Default is used.
        variables: dict
            Other values to pass to config.
        """
        if config and (locator or acl):
            raise ValueError("It is not allowed to use 'config' parameter along with 'locator' or 'acl'")

        # builders that construct locator and acl based on parameters
        acl_builder = ACLBuilder(src=acl, clean_up=clean_up, copy=copy, session_ref=session_ref)
        locator_builder = LocatorBuilder(src=locator, clean_up=clean_up, copy=copy, session_ref=session_ref)
        config_copied = True

        if config:
            # copy passed file, we should not work with externally passed files
            if copy and not os.getenv('OTP_WEBAPI_TEST_MODE'):
                self._file = utils.TmpFile(".cfg", clean_up=clean_up)
                config_path = config.path if isinstance(config, Config) else config

                shutil.copyfile(config_path, self._file.path)
            else:
                self._file = utils.PermanentFile(config)
                config_copied = False

            if utils.is_param_in_config(self._file.path, "ACCESS_CONTROL_FILE"):
                acl_builder.src = utils.get_config_param(self._file.path, "ACCESS_CONTROL_FILE")

            if utils.is_param_in_config(self._file.path, "DB_LOCATOR.DEFAULT"):
                locator_builder.src = utils.get_config_param(self._file.path, "DB_LOCATOR.DEFAULT")

        else:
            self._file = utils.tmp_config(clean_up=clean_up)

        self._acl = acl_builder.build()
        # it is used in onetick-py-test
        os.environ["ONE_TICK_SESSION_ACL_PATH"] = self._acl.path
        self._locator = locator_builder.build()
        # it is used in onetick-py-test
        os.environ["ONE_TICK_SESSION_LOCATOR_PATH"] = self._locator.path

        super().__init__(self._file, clean_up=clean_up, copied=config_copied, session_ref=session_ref)

        # Here we can start to modify files - they are either copied or generated
        # ------------------------------------------------------------------------

        utils.modify_config_param(self.path, "ACCESS_CONTROL_FILE", self._acl.path, throw_on_missing=False)
        utils.modify_config_param(self.path, "DB_LOCATOR.DEFAULT", self._locator.path, throw_on_missing=False)

        if otq_path:
            otq_path = map(str, otq_path)
            utils.modify_config_param(self.path, "OTQ_FILE_PATH", ",".join(otq_path), throw_on_missing=False)
        if csv_path:
            csv_path = map(str, csv_path)
            utils.modify_config_param(self.path, "CSV_FILE_PATH", ",".join(csv_path), throw_on_missing=False)
        variables = variables or {}
        for parameter_name, parameter_value in variables.items():
            if parameter_name in self._CONFIG_VARIABLES_PASSED_VIA_THEIR_OWN_PARAMETER:
                raise ValueError(f'Variable {parameter_name} should be set via '
                                 f'{self._CONFIG_VARIABLES_PASSED_VIA_THEIR_OWN_PARAMETER[parameter_name]} parameter')
            if isinstance(parameter_value, list):
                parameter_value = ",".join(map(str, parameter_value))
            utils.modify_config_param(self.path, parameter_name, parameter_value, throw_on_missing=False)

        # set license
        # ---------------------------
        custom_license = utils.is_param_in_config(self.path, "LICENSE_REPOSITORY_DIR")
        custom_license &= utils.is_param_in_config(self.path, "ONE_TICK_LICENSE_FILE")
        if license:
            self._license = license
        else:
            if custom_license:
                lic_file = utils.get_config_param(self.path, "ONE_TICK_LICENSE_FILE")
                lic_dir = utils.get_config_param(self.path, "LICENSE_REPOSITORY_DIR")
                self._license = _license.Custom(lic_file, lic_dir)
            else:
                if isinstance(locator, _servers.RemoteTS):
                    self._license = _license.Remote()
                else:
                    self._license = _license.Default()

        if not custom_license:  # no need to set already defined custom values
            if self._license.dir:
                utils.modify_config_param(self.path,
                                          "LICENSE_REPOSITORY_DIR",
                                          self._license.dir,
                                          throw_on_missing=False)
            if self._license.file:
                utils.modify_config_param(self.path,
                                          "ONE_TICK_LICENSE_FILE",
                                          self._license.file,
                                          throw_on_missing=False)

    @property
    def acl(self):
        return self._acl

    @property
    def locator(self):
        return self._locator

    @property
    def license(self):
        return self._license

    def copy(self, clean_up=utils.default, copy=True, session_ref=None):
        """ overridden version of copy """
        return self.__class__(self.path, clean_up=clean_up, copy=copy, session_ref=session_ref, license=self._license)

    @staticmethod
    def build(obj=None, clean_up=utils.default, copy=True, session_ref=None):
        params = {"clean_up": clean_up, "copy": copy, "session_ref": session_ref}

        if isinstance(obj, str):
            return Config(obj, **params)
        elif isinstance(obj, utils.File):
            return Config(obj.path, **params)
        elif isinstance(obj, Config):
            return obj.copy(**params)
        elif obj is None:
            return Config(**params)

        raise ValueError(f'It is not allowed to build Config from the object of type "{type(obj)}"')

    def cleanup(self):
        # no logic to clean up content

        self._acl.cleanup()
        self._locator.cleanup()

    @property
    def otq_path(self):
        try:
            return utils.get_config_param(self.path, "OTQ_FILE_PATH")
        except AttributeError:
            return None


class PerformanceMetricsParser:
    metrics_fields = {
        "user_time": float,
        "system_time": float,
        "elapsed_time": float,
        "virtual_memory": int,
        "virtual_memory_peak": int,
        "working_set": int,
        "working_set_peak": int,
        "disk_read": int,
        "disk_write": int,
    }

    metrics_units = {
        "user_time": "s",
        "system_time": "s",
        "elapsed_time": "s",
        "virtual_memory": "bytes",
        "virtual_memory_peak": "bytes",
        "working_set": "bytes",
        "working_set_peak": "bytes",
        "disk_read": "bytes",
        "disk_write": "bytes",
    }

    def __init__(self):
        self._metrics = {}

    def parse(self, log_file: str):
        with open(log_file, "r") as log_file_io:
            for log_line in log_file_io:
                result = re.search(r"Performance Metrics FINAL ([\w\s]+) \((\w+)\): (.+)$", log_line.strip())
                if not result:
                    continue

                metric_name, metric_units, metric_value = result.groups()
                metric_key = "_".join(metric_name.lower().split())

                if metric_key not in self.metrics_fields:
                    raise ValueError(
                        f"Unexpected performance metric `{metric_key}` was found in the log: \"{log_line.strip()}\""
                    )

                if metric_units != self.metrics_units[metric_key]:
                    raise ValueError(
                        f"Unexpected units for metric `{metric_key}`: "
                        f"expected `{self.metrics_units[metric_key]}`, got `{metric_units}`."
                    )

                if metric_key in self._metrics:
                    raise KeyError(f"Metric `{metric_key}` was already saved, but another entry was found in the log.")

                try:
                    metric_value = self.metrics_fields[metric_key](metric_value)
                except Exception as exc:
                    raise ValueError(
                        f"Failed to parse and convert metric {metric_key} "
                        f"from `str` to `{self.metrics_fields[metric_key].__name__}`: \"{metric_value}\""
                    ) from exc

                self._metrics[metric_key] = {
                    "name": metric_name,
                    "value": metric_value,
                    "units": self.metrics_units[metric_key],
                }

        not_found_keys = set(self.metrics_fields.keys()).difference(self._metrics.keys())
        if not_found_keys:
            raise RuntimeError("Expected performance metrics not found in the log: " + ", ".join(not_found_keys))

    @property
    def metrics(self):
        return self._metrics.copy()


class Session:
    """
    A class for setting up working OneTick session. It keeps configuration files during
    the session and allows to manage them. When instance is out of scope, then instance
    cleans up config files and configuration.
    You can leave the scope manually with method :py:meth:`close`.
    Also, session is closed automatically if this object is used as a context manager.

    .. note::
        It is allowed to have only one alive session instance in the process.

    If you don't use Session's instance, then ``ONE_TICK_CONFIG`` environment variable
    should be set to be able to work with OneTick.

    If config file is not set then temporary is generated.
    Config includes locator and acl file, and if they are not set, then they are generated.

    Parameters
    ----------
    config : str, :py:class:`onetick.py.session.Config`, optional
        Path to an existing OneTick config file; if it is not set, then config will be generated.
        If config is not set, then temporary config is generated. Default is None.
    clean_up : bool, optional
        A flag to control cleaning up process: if it is True then all temporary generated files will
        be automatically removed. It is helpful for debugging. The flag affects only generated files, but
        does not externally passed.

        By default,
        :py:attr:`otp.config.clean_up_tmp_files<onetick.py.configuration.Config.clean_up_tmp_files>` is used.
    copy : bool, optional
        A flag to control file copy process: if it is True then all externally passed files will be
        copied before usage, otherwise all modifications during an existing session happen directly with
        passed config files. NOTE: we suggest to set this flag only when you fully understand it's effect.
        Default is True.
    override_env : bool, optional
        If flag is True, then unconditionally ``ONE_TICK_CONFIG`` environment variable will be overridden
        with a config that belongs to a Session. Otherwise ``ONE_TICK_CONFIG``
        will be defined in the scope of session only when it is not defined externally.
        For example, it is helpful when you test ascii_loader that uses 'ONE_TICK_CONFIG' only.

        Default is False ( default is False, because overriding external environment variable
        might be not obvious and desirable )
    redirect_logs: bool, optional
        If flag is True, then OneTick logs  will be redirected into a temporary log file. Otherwise
        logs will be mixed with output. Default is True.
    gather_performance_metrics: bool, optional
        If flag is True, then enables performance metrics gathering, by setting ``DUMP_PERF_METRICS`` config parameter.
        Sets ``redirect_logs`` flag to ``True``.

        Metrics are available after closing a session via ``session.performance_metrics`` property.

        .. warning::
            Due to current limitations, metrics are cumulative. So if you run multiple queries in the same process
            (even with different session objects), you'll get metrics for the whole process since it's start
            till end of a session with ``gather_performance_metrics=True``.

            To avoid this, you can create a session in a separate process, either by using Python's ``multiprocessing``
            or by moving required code to a separate Python script and running it in a new process.

        .. note::
            Metrics are gathered for all operations in the session between its creation and closing.

    Examples
    --------

    If session is defined with environment, OneTick can be used right away:

    >>> 'ONE_TICK_CONFIG' in os.environ
    True
    >>> list(otp.databases()) # doctest: +ELLIPSIS
    ['COMMON', 'DEMO_L1', ..., 'SOME_DB', 'SOME_DB_2'...
    >>> data = otp.DataSource('SOME_DB', symbol='S1', tick_type='TT')
    >>> otp.run(data)
                         Time  X
    0 2003-12-01 00:00:00.000  1
    1 2003-12-01 00:00:00.001  2
    2 2003-12-01 00:00:00.002  3

    Collecting performance metrics with ``gather_performance_metrics`` parameter:

    >>> with otp.Session(gather_performance_metrics=True) as session:  # doctest: +SKIP
    >>>    data_a = otp.DataSource('DB_A', symbol='S1', tick_type='TT')  # doctest: +SKIP
    >>>    data_b = otp.DataSource('DB_B', symbol='S1', tick_type='TT')  # doctest: +SKIP
    >>>    _ = otp.run(otp.merge([data_a, data_b]))  # doctest: +SKIP
    >>>
    >>> session.performance_metrics  # doctest: +SKIP
    {
        'user_time': {'name': 'User Time', 'value': 3.39063, 'units': 's'},
        'system_time': {'name': 'System Time', 'value': 1.07813, 'units': 's'},
        'elapsed_time': {'name': 'Elapsed Time', 'value': 6.78816, 'units': 's'},
        'virtual_memory': {'name': 'Virtual Memory', 'value': 6261944320, 'units': 'bytes'},
        'virtual_memory_peak': {'name': 'Virtual Memory Peak', 'value': 6271926272, 'units': 'bytes'},
        'working_set': {'name': 'Working Set', 'value': 228110336, 'units': 'bytes'},
        'working_set_peak': {'name': 'Working Set Peak', 'value': 228126720, 'units': 'bytes'},
        'disk_read': {'name': 'Disk Read', 'value': 32289438, 'units': 'bytes'},
        'disk_write': {'name': 'Disk Write', 'value': 172906, 'units': 'bytes'}
    }
    """
    # TODO: create article for Session in Guides or Concepts

    _instance = None

    def __init__(
        self, config=None, clean_up=utils.default, copy=True, override_env=False, redirect_logs=True,
        gather_performance_metrics=False,
    ):
        self._construct(config, clean_up, copy, override_env, redirect_logs, gather_performance_metrics)

    def _construct(
        self, config=None, clean_up=utils.default, copy=True, override_env=False, redirect_logs=True,
        gather_performance_metrics=False,
    ):

        if Session._instance:
            raise MultipleSessionsException(
                "It is forbidden to use multiple active sessions simultaniously in one process"
            )

        def onetick_cfg_rollback(var):
            """
            function to rollback ONE_TICK_CONFIG state
            """

            def _impl():
                if var is None:
                    if "ONE_TICK_CONFIG" in os.environ:
                        del os.environ["ONE_TICK_CONFIG"]
                else:
                    os.environ["ONE_TICK_CONFIG"] = var

            return _impl

        self._lib = None
        self._env_rollback = onetick_cfg_rollback(os.environ.get("ONE_TICK_CONFIG", None))
        self._override_env = override_env

        self._config = Config.build(config, clean_up=clean_up, copy=copy, session_ref=self)
        # it is used in onetick-py-test
        os.environ["ONE_TICK_SESSION_CFG_PATH"] = self._config.path

        try:
            if "ONE_TICK_CONFIG" not in os.environ:
                os.environ["ONE_TICK_CONFIG"] = self._config.path
            else:
                if override_env:
                    os.environ["ONE_TICK_CONFIG"] = self._config.path
                else:
                    warnings.warn(
                        UserWarning(
                            "ONE_TICK_CONFIG env variable has been set before a session, "
                            "and in the session scope it is not related to the session config. "
                            "If you want to make ONE_TICK_CONFIG env variable be consistent "
                            "with the session, then look at the override_env flag "
                            "for the Session constructor"
                        )
                    )

            otli.OneTickLib().cleanup()

            self._performance_metrics_parser = None
            if gather_performance_metrics:
                self._performance_metrics_parser = PerformanceMetricsParser()
                utils.modify_config_param(
                    self._config.path, "DUMP_PERF_METRICS", "ON", throw_on_missing=False,
                )
                redirect_logs = True

            self._log_file = log_file = None
            if redirect_logs:
                self._log_file = utils.TmpFile(suffix=".onetick.log", clean_up=clean_up)
                log_file = self._log_file.path

            self._lib = otli.OneTickLib(self._config.path, log_file=log_file)
        except Exception:
            self._env_rollback()
            # TODO: rollback, but need to wait BDS-91
            raise

        # force reload cfg/locator/acl, because it is not reloaded after each time session if re-created
        if os.getenv('OTP_WEBAPI_TEST_MODE'):
            utils.reload_config(None, config_type='MAIN_CONFIG')
            self.locator.reload()
            self.acl.reload()

        self._ts_dbs = {}

        # PY-1352: this line must be the last
        # in case we get exception anywhere before, we shouldn't set Session class variable,
        # because it will affect the creation of all the future Session objects
        Session._instance = self

    def use(self, *items):
        """
        Makes DB or TS available inside the session.

        Parameters
        ----------
        items : :py:class:`~onetick.py.DB` or :py:class:`~onetick.py.servers.RemoteTS` objects
            Items to be added to session.

        Examples
        --------

        (note that ``session`` is created before this example)

        >>> list(otp.databases()) # doctest: +ELLIPSIS
        ['COMMON', 'DEMO_L1', ...]
        >>> new_db = otp.DB('ZZZZ')
        >>> session.use(new_db)
        >>> list(otp.databases()) # doctest: +ELLIPSIS
        ['COMMON', 'DEMO_L1', ..., 'ZZZZ']
        """
        self.locator.add(*items)
        dbs = []
        for item in items:
            if isinstance(item, _db.db._DB):
                dbs.append(item)
        try:
            if dbs:
                self.acl.add(*dbs)
        except Exception:
            self.locator.remove(*items)
            raise

    def use_stub(self, stub_name):
        """
        Adds stub-DB into the session.
        The shortcut for ``.use(otp.DB(stub_name))``

        Parameters
        ----------
        stub_name : str
            name of the stub
        """
        return self.use(_db.DB(stub_name))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            # session will be closed due to exception, no need to parse performance metrics
            self._performance_metrics_parser = None

        self.close()

    @staticmethod
    def _available_dbs():
        return _databases()

    def _get_ts_dbs(self):
        locator_dbs = self.locator.databases

        all_dbs = self._available_dbs()

        for db_name in all_dbs:
            if db_name not in locator_dbs and db_name not in self._ts_dbs:
                self._ts_dbs[db_name] = _db.db._DB(db_name)

    def close(self):
        """
        Close session
        """
        if Session._instance == self:
            try:
                if self._config:
                    del self._config
                    self._config = None

            finally:
                if self._lib:
                    self._lib.cleanup()
                    self._lib = None

                self._env_rollback()

                Session._instance = None

                if self._performance_metrics_parser:
                    self._performance_metrics_parser.parse(self._log_file)

    def __del__(self):
        self.close()

    @property
    def config(self):
        """
        A reference to the underlying Config object that represents OneTick config file.

        Returns
        -------
        :py:class:`onetick.py.session.Config`
        """
        return self._config

    @config.setter
    def config(self, cfg):
        self.close()
        self._construct(cfg, override_env=self._override_env)

    @property
    def acl(self):
        """
        A reference to the underlying ACL object that represents OneTick access control list file.

        Returns
        -------
        :py:class:`onetick.py.session.ACL`
        """
        return self._config.acl

    @property
    def locator(self):
        """
        A reference to the underlying Locator that represents OneTick locator file.

        Returns
        -------
        :py:class:`onetick.py.session.Locator`
        """
        return self._config.locator

    @property
    def license(self):
        return self._config.license

    @property
    def ts_databases(self):
        self._get_ts_dbs()
        return self._ts_dbs

    @property
    def databases(self):
        return self._available_dbs()

    @property
    def performance_metrics(self):
        if self._instance:
            raise RuntimeError("Trying to get performance metrics before closing the session.")

        if not self._performance_metrics_parser:
            raise RuntimeError(
                "Trying to get performance metrics, "
                "however session was created without `gather_performance_metrics=True`."
            )

        return self._performance_metrics_parser.metrics


class TestSession(Session):
    def __init__(self, *args, **kwargs):
        """
        This class does the same as :py:class:`onetick.py.session.Session`,
        but also defines default required :py:attr:`otp.config <onetick.py.configuration.Config>` values.

        Using this session object is the equivalent of defining these configuration values:

        ::

            otp.config['tz'] = 'EST5EDT'
            otp.config['default_db'] = 'DEMO_L1'
            otp.config['default_symbol'] = 'AAPL'
            otp.config['default_start_time'] = datetime(2003, 12, 1, 0, 0, 0)
            otp.config['default_end_time'] = datetime(2003, 12, 4, 0, 0, 0)

        ``DEMO_L1`` is the database defined in the default locator generated by onetick.py
        and it is also a name of the database commonly used in the OneTick ecosystem, academy courses, etc.
        """
        configuration.config['tz'] = 'EST5EDT'
        configuration.config['default_db'] = 'DEMO_L1'
        configuration.config['default_symbol'] = 'AAPL'
        configuration.config['default_start_time'] = datetime(2003, 12, 1, 0, 0, 0)
        configuration.config['default_end_time'] = datetime(2003, 12, 4, 0, 0, 0)
        super().__init__(*args, **kwargs)


class HTTPSession:
    param_list = ['http_address', 'http_username', 'http_password', 'access_token', 'http_proxy', 'https_proxy']

    def __init__(self,
                 http_address,
                 http_username=None,
                 http_password=None,
                 access_token=None,
                 http_proxy=None,
                 https_proxy=None):
        """
        This class must be used only for WebAPI connection,
        to set HTTP connection parameters.
        """
        import onetick.py as otp  # noqa
        self._restore_config = {}
        for param in self.param_list:
            if locals()[param]:
                self._restore_config[param] = otp.config.get(param)
                otp.config.__setattr__(param, locals()[param])

    def close(self):
        # restore config
        import onetick.py as otp  # noqa
        for param, value in self._restore_config.items():
            otp.config.__setattr__(param, value)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
