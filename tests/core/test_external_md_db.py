import pytest
from datetime import datetime

import onetick.py as otp

from onetick.py.sources import NBBO, Trades
from onetick.py.functions import join_by_time

from onetick.py.db import DB
from onetick.py.core import db_constants as constants


TAQ_NBBO = DB(
    name="TAQ_NBBO",
    db_properties={
        "symbology": "BZX",
        "price_not_key": True,
        "memory_data_max_life_hours": 30,
        "memory_db_dir": "/onetick-tickdata-com/STORAGE_GATEWAY/DEEP_HISTORY/US_TED/NBBO/shmem",
        "mmap_db_compression_type": constants.compression_type.NATIVE_PLUS_GZIP,
    },
    db_locations=[
        {
            "access_method": constants.access_method.FILE,
            "location": "/onetick-tickdata-com/STORAGE_GATEWAY/DEEP_HISTORY/US_TED/NBBO/",
            "start_time": datetime(year=2001, month=1, day=1),
            "end_time": constants.DEFAULT_END_DATE,
        }
    ],
)


US_COMP = DB(
    name="US_COMP",
    db_properties={
        "symbology": "BZX",
        "price_not_key": True,
        "memory_data_max_life_hours": 30,
        "memory_db_dir": "/onetick-tickdata-com/STORAGE_GATEWAY/DEEP_HISTORY/US_TED/TAQ/shmem",
        "mmap_db_compression_type": constants.compression_type.NATIVE_PLUS_GZIP,
    },
    db_locations=[
        {
            "access_method": constants.access_method.FILE,
            "location": "/onetick-tickdata-com/STORAGE_GATEWAY/DEEP_HISTORY/US_TED/TAQ/",
            "start_time": datetime(year=2003, month=10, day=1),
            "end_time": constants.DEFAULT_END_DATE,
        }
    ],
)


@pytest.mark.platform("linux")
def test_1(f_session):
    f_session.use(US_COMP)
    f_session.use(TAQ_NBBO)

    nbbo = NBBO(db=TAQ_NBBO, start=datetime(2017, 6, 7, 12), end=datetime(2017, 6, 7, 12, 1))
    nbbo = nbbo[[nbbo.ASK_PRICE, nbbo.BID_PRICE]]

    trds = Trades(db=US_COMP, start=datetime(2017, 6, 8, 12), end=datetime(2017, 6, 8, 12, 1))
    trds = trds[[trds.PRICE]]
    trds.PREV_TS = trds.TIMESTAMP
    trds.TIMESTAMP -= 24 * 60 * 60 * 1000

    joined_data = join_by_time([trds, nbbo])

    print(otp.run(joined_data))
