import os

from onetick.py.otq import otli
from onetick.py.db import DB
from onetick.py.session import Session


class TmpSession:
    def __init__(self):
        if Session._instance is None:
            if "ONE_TICK_CONFIG" in os.environ:
                # use external session
                otli.OneTickLib()
                self.session = None
                self.session_is_owned = False
            else:
                self.session_is_owned = True
                self.session = Session()

        else:
            self.session_is_owned = False
            self.session = Session._instance
            self.temp_dbs = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:  # exclude case when session is set externally, ie using ONE_TICK_CONFIG
            if self.session_is_owned:
                self.session.close()
            else:
                for db in self.temp_dbs:
                    self.session.locator.remove(db)

    def __db_is_registered(self, db):
        base_db = str(db).split("//")[0]    # noqa
        return (base_db in self.session.databases) or (base_db in self.session.locator.databases)

    def use(self, db):
        if self.session is None and isinstance(db, str) and "ONE_TICK_CONFIG" in os.environ:
            # we assume that database is already in the locator, that is
            # managed externally by user, and we don't need to add it
            # into the locator
            return

        if not isinstance(db, (str, DB)):
            raise TypeError(
                "Only a DB object or a string can be passed as db into get_"
                f"functions working with databases. Instead, {db.__class__} was provided."
            )

        if isinstance(db, str) and not self.__db_is_registered(db):
            if self.session_is_owned:
                raise TypeError(
                    "When there is no active session, db argument must be a DB object, not a database name."
                )
            else:
                raise TypeError(f"We can not find passed database {db} in the session")

        if not self.__db_is_registered(db):
            self.session.locator.add(db)
            if not self.session_is_owned:
                self.temp_dbs.append(db)
