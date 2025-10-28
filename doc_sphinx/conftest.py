from datetime import datetime
from pprint import pprint
from functools import partial
from collections import defaultdict

import os
import pytest
import pandas
import onetick.py as otp
from onetick.py.otq import otq


@pytest.fixture(scope='session')
def real_db_schemas():
    """ A temporary session that helps to get real database schemas to
    generate realistic ticks """

    res = {}

    with otp.Session() as session:
        servers = otp.RemoteTS(
            otp.LoadBalancing(
                "development-queryhost.preprod-solutions.parent.onetick.com:50015",
                "development-queryhost-2.preprod-solutions.parent.onetick.com:50015"
            )
        )

        session.use(servers)

        res['us_comp_trd'] = otp.databases()['US_COMP'].schema(
            tick_type='TRD',
            date=otp.dt(2022, 3, 2)
        )

        res['us_comp_qte'] = otp.databases()['US_COMP'].schema(
            tick_type='QTE',
            date=otp.dt(2022, 3, 2)
        )

    return res


@pytest.fixture(scope='module')
def session(pytestconfig, real_db_schemas):

    def add_data(db, *args, **kwargs):
        """ Facade that helps to prevent a case when existing data is ovewritten """

        key = ''.join(map(str, args[1:])) + ''.join(f'{key}_{value}' for key, value in kwargs.items())

        if key in db.keys:
            raise ValueError(f'Data {key} for {db.name} already exists, changing it might affect other tests')

        db._add(*args, **kwargs)

        db.keys.add(key)

    class DBsDict(dict):
        """ Proxy-helper that simplifies add and access existing databases """

        def __getitem__(self, db_name):
            if db_name not in self:
                db_obj = otp.DB(db_name)
                db_obj.keys = set()
                db_obj._add = db_obj.add
                db_obj.add = partial(add_data, db_obj)

                super().__setitem__(db_name, db_obj)
                self.session.use(db_obj)

            return super().__getitem__(db_name)

    config = otp.Config(
        otq_path=[
            os.path.join(pytestconfig.rootdir, 'doctest_resources')
        ]
    )

    with otp.Session(config) as session:
        session.dbs = DBsDict()
        session.dbs.session = session
        session.real_db_schemas = real_db_schemas

        yield session


@pytest.fixture(autouse=True)
def add_session(doctest_namespace, request, session, monkeypatch, mocker):
    doctest_namespace['otq'] = otq
    doctest_namespace['otp'] = otp
    doctest_namespace['session'] = session
    doctest_namespace['datetime'] = datetime
    doctest_namespace['pd'] = pandas
    doctest_namespace['pprint'] = pprint
    doctest_namespace['monkeypatch'] = monkeypatch
    doctest_namespace['mocker'] = mocker
