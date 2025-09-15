import pytest
from multiprocessing import Process, Queue
import threading
import subprocess
import time
import os
import psutil
from datetime import datetime, timezone, timedelta
from dateutil import tz

import onetick.py as otp


@pytest.fixture
def session():
    with otp.Session() as s:
        db = otp.DB('TEST_DB', kind='accelerator')
        s.use(db)
        s.db = db
        yield s


class BaseRunning:

    def run(self):  # noqa
        self.run = True

        thread = threading.Thread(target=self._check_loop)
        thread.start()

        self.running_query()

        self.run = False
        thread.join()

    def _check_loop(self):
        while self.run:
            self.check_loop()

    def running_query(self):
        raise NotImplementedError()


class TestAcceleratorDbWrite(BaseRunning):
    """
    A case when the running query writes into the accellerator database.
    A running query generates every 1 second a tick with X=1 value in real time
    and writes into the accelerator database.
    Test checks in a separated thead that tick is written in the database every second.
    """

    @pytest.fixture
    def session(self):
        self.prev_num_ticks = None
        self.diffs = []

        with otp.Session() as s:
            db = otp.DB('TEST_DB', kind='accelerator')
            s.use(db)
            s.db = db
            self.data = otp.DataSource('TEST_DB', tick_type='TT', symbol='S', schema_policy='manual')
            yield s

    def check_loop(self):
        time.sleep(1)

        # count the ticks in the database
        now = otp.dt.now(tz='GMT')
        result = otp.run(self.data,
                         start=now - otp.Minute(1),
                         end=now + otp.Minute(1),
                         timezone='GMT')
        num_ticks = len(result)
        if self.prev_num_ticks is not None:
            # store the number of ticks that were added each second
            diff = num_ticks - self.prev_num_ticks
            self.diffs.append(diff)
        self.prev_num_ticks = num_ticks

    def running_query(self):
        data = otp.Tick(X=1, bucket_interval=1)
        data = data.write('TEST_DB', symbol='S', tick_type='TT')

        otp.run(data,
                running=True,
                start=otp.now(),
                end=otp.now() + otp.Second(10),
                timezone='GMT')
        assert self.prev_num_ticks == 10
        # let's check if there are at least 2 times when number of ticks was changed
        # ideally, it would always be 10 (1 new tick each second),
        # but in reality the test and its threads may run slower
        assert len([x for x in self.diffs if x > 0]) >= 2

    def test_tick(self, session):
        self.run()


@pytest.mark.skipif(os.name == "nt", reason="It is not stable for windows")
def test_dump():
    ''' A case that allows to check in real time that running query works and generates
    outut in stdout every one second. Illustrates also how the `start` works when it is
    set to the past '''
    with otp.Session():
        data = otp.Tick(X=otp.rand(min_value=1, max_value=5), bucket_interval=1)

        data.dump()

        print()
        print('----')
        print('Now : ', datetime.now(tz=timezone.utc).strftime("%d/%m/%Y %H:%M:%S.%f"))
        print('----')

        otp.run(data,
                running=True,
                start=otp.now() - otp.Second(10),
                end=otp.now() + otp.Second(10))


@pytest.fixture
def tick_server():
    ''' Tick server process as a separate process to make sure that it will
    be stopped in the teardown.
    The tick server works as a service for in-memory database, and provides readers access
    to that database. '''
    def run_server(q):
        shmem_dir = otp.utils.TmpDir(suffix='.shmem')
        with otp.Session() as s:
            db = otp.DB('TEST_DB',
                        db_properties=dict(memory_db_dir=shmem_dir,
                                           memory_data_max_life_hours=30),
                        db_feed={'type': 'memdb',
                                 'tick_types_list': 'TT',
                                 'new_data_check_interval_msec': 500,
                                 'heartbeat_interval_msec': 500})
            s.use(db)

            ot_path = os.path.join(otp.utils.omd_dist_path(), 'one_tick', 'bin', 'tick_server.exe')
            with subprocess.Popen([ot_path, '-port', '47001'],
                                  env=dict(ONE_TICK_CONFIG=os.environ['ONE_TICK_CONFIG'])) as p:
                q.put(p.pid)  # put the process to have ability to kill it externally
                q.put(os.environ['ONE_TICK_CONFIG'])

    queue = Queue()
    tick_server = Process(target=run_server, args=(queue,))
    tick_server.start()

    tick_server_pid = queue.get()
    tick_server_cfg = queue.get()

    yield tick_server_cfg

    p = psutil.Process(tick_server_pid)
    p.terminate()
    p.wait()


@pytest.fixture
def writer(tick_server):
    ''' Local process that writes into the memory database using the running query '''
    def cep_query(q, cfg):
        with otp.Session(cfg):
            data = otp.Tick(X=otp.rand(1, 100), bucket_interval=1)
            # `date` is set to None that indicates we are writing into the memory db
            data = data.write('TEST_DB', symbol='S', tick_type='TT', date=None)
            otp.run(data,
                    running=True,
                    start=otp.now(),
                    end=otp.now() + otp.Second(20))

    queue = Queue()
    running_query = Process(target=cep_query, args=(queue, tick_server,))
    running_query.start()

    yield

    running_query.terminate()
    running_query.join()


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Binaries (tick_server.exe) is not available in WebAPI')
@pytest.mark.skipif(os.name == "nt", reason="We do not have OneTick server on the windows")
def test_in_memory_db(tick_server, writer):
    """
    Test checks how we could write and read from the memory db.
    The test has three components:
    - a local process that runs the CEP query and writes into the memory db
    - a tick server prorcess that points to that memory db and provides access for read
    - a process that points to the tick server and read data from the memory db
    """
    remote = otp.RemoteTS('localhost:47001')
    with otp.Session(otp.Config(locator=remote)):

        def get_source():
            return otp.DataSource('TEST_DB',
                                  symbol='S',
                                  tick_type='TT',
                                  schema_policy='manual',
                                  start=otp.dt.now() - otp.Day(1),
                                  end=otp.dt.now() + otp.Day(1))

        assertion = False
        prev_count = 0
        for _ in range(10):
            time.sleep(1)

            data = get_source()
            df = otp.run(data)
            print(df)

            if not df.empty:
                assert 'X' in df
                assert len(list(df)) == 2
                assert len(df) - prev_count >= 1
                prev_count = len(df)
                assertion = True

        if not assertion:
            raise ValueError("Couldn't get data after 10 iterations")


@pytest.mark.skipif(os.getenv('OTP_WEBAPI_TEST_MODE', False),
                    reason='Binaries (tick_server.exe) is not available in WebAPI')
@pytest.mark.skipif(os.name == "nt", reason="We do not have OneTick server on the windows")
def test_cep_perf(tick_server, writer):
    with otp.Session(tick_server):
        data = otp.DataSource(tick_type='TT')
        otq_file = data.to_otq(start=otp.now(), end=otp.now() + otp.Second(5), symbols='TEST_DB::S', running=True)
        result = otp.perf.MeasurePerformance(otq_file)
        assert result.cep_summary.text is not None
        assert len(result.cep_summary.entries) == 1
        assert result.cep_summary.entries[0].sink_ep_name


@pytest.mark.skip(reason='Some bug with memdb CEP adapter not stopping -- need to report to devs')
def test_default_db_tick_generator():
    with otp.Session() as s:
        mem_db = 'TEST_MEMDB'
        s.use(
            otp.DB(
                mem_db,
                db_properties=dict(memory_db_dir=otp.utils.TmpDir()),
                db_feed=dict(type='memdb'),
            )
        )
        src1 = otp.DataSource(db=mem_db, schema_policy='manual', schema={'A': int})
        src2 = otp.Tick(A=1, bucket_interval=1)

        src = src1 + src2
        res = otp.run(src, symbols='AAPL', start=otp.now(), end=otp.now() + otp.Second(10), running=True)
        assert len(res) == 10


class TestSubqueries:

    TIMEZONE = 'America/New_York'

    def test_fsq(self, session):
        fsq = otp.Tick(SYMBOL_NAME='AAPL')
        main_query = otp.Tick(DUMMY=1, bucket_interval=1)
        main_query['SN'] = main_query['_SYMBOL_NAME']
        tzinfo = tz.gettz(self.TIMEZONE)
        current_time = datetime.now(tz=tzinfo)
        start_time = current_time + timedelta(seconds=5)
        end_time = start_time + timedelta(seconds=10)
        res = otp.run(main_query,
                      symbols=fsq,
                      running=True,
                      start=start_time,
                      end=end_time,
                      timezone=self.TIMEZONE)['AAPL']
        assert len(res) == 10

    def test_jwq(self, session):
        main_query = otp.Tick(DUMMY=1, bucket_interval=1)
        main_query['SN'] = main_query['_SYMBOL_NAME']

        jwq_query = otp.Tick(JWQ_DUMMY=2)
        jwq_query['JWQ_ST'] = jwq_query['_START_TIME']

        main_query = main_query.join_with_query(
            jwq_query,
            start=main_query['_START_TIME'] - otp.Hour(1),
            end=main_query['_START_TIME'] - otp.Hour(1) + otp.Minute(1),
        )

        tzinfo = tz.gettz(self.TIMEZONE)
        current_time = datetime.now(tz=tzinfo)
        start_time = current_time + timedelta(seconds=5)
        end_time = start_time + timedelta(seconds=10)
        res = otp.run(main_query, symbols='AAPL', running=True, start=start_time, end=end_time, timezone=self.TIMEZONE)
        assert len(res) == 10
        for i in range(0, 10):
            assert res['JWQ_ST'][i] == (start_time - otp.timedelta(hours=1)).replace(tzinfo=None)
