Aggregations
============

.. _aggregations_funcs:

This section contains reference for the available aggregation functions.

However here are listed some basic examples of using parameters common to different aggregations.

How aggregation buckets are created
-----------------------------------

.. _buckets_guide:

Each aggregation is split into "buckets" - time intervals in which the data is aggregated.

The creation of buckets is determined by the query time interval
and by several aggregation parameters: ``bucket_interval``, ``running`` and ``all_fields``.

Let's see all different combinations of them and how they produce different results.

First, the input data, 10 ticks with different offsets, query time interval is 1 minute:

.. doctest::

   >>> t = otp.Ticks(A=range(10),
   ...               start=otp.dt(2003, 12, 1),
   ...               end=otp.dt(2003, 12, 1) + otp.Minute(1),
   ...               offset=[otp.Second(i) for i in [0, 5, 13, 15, 20, 27, 30, 35, 42, 45]])
   >>> otp.run(t)
                    Time  A
   0 2003-12-01 00:00:00  0
   1 2003-12-01 00:00:05  1
   2 2003-12-01 00:00:13  2
   3 2003-12-01 00:00:15  3
   4 2003-12-01 00:00:20  4
   5 2003-12-01 00:00:27  5
   6 2003-12-01 00:00:30  6
   7 2003-12-01 00:00:35  7
   8 2003-12-01 00:00:42  8
   9 2003-12-01 00:00:45  9

Fixed-size buckets
^^^^^^^^^^^^^^^^^^

By default, ``bucket_interval`` is set to 0 and ``running`` is set to False.
It aggregates the input data in a single bucket from the query start time to the query end time,
producing a single output tick:

.. doctest::

   >>> otp.run(
   ...     t.agg({'S': otp.agg.sum('A')})
   ... )
                    Time   S
   0 2003-12-01 00:01:00  45

Setting ``bucket_interval`` to some value will split the query time interval into buckets
of the specified fixed size
(except the last bucket that may be smaller if the time interval can't be evenly divided).
Note that bucket intervals are non-overlapping and bucket end time is exclusive, so the tick
with the timestamp equal to the bucket end time will be included in the next bucket.

.. doctest::

   >>> otp.run(
   ...     t.agg({'S': otp.agg.sum('A')}, bucket_interval=21)
   ... )
                    Time   S
   0 2003-12-01 00:00:21  10
   1 2003-12-01 00:00:42  18
   2 2003-12-01 00:01:00  17

Sliding window (parameter ``running``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Setting ``running`` to True will aggregate the input data in a "sliding window".

Setting ``running`` to True without setting ``bucket_interval``
will basically create a "cumulative" aggregation.
It will create a bucket for each input tick from the query start time to the input tick's timestamp (inclusive):

.. doctest::

   >>> otp.run(
   ...     t.agg({'S': otp.agg.sum('A')}, running=True)
   ... )
                    Time   S
   0 2003-12-01 00:00:00   0
   1 2003-12-01 00:00:05   1
   2 2003-12-01 00:00:13   3
   3 2003-12-01 00:00:15   6
   4 2003-12-01 00:00:20  10
   5 2003-12-01 00:00:27  15
   6 2003-12-01 00:00:30  21
   7 2003-12-01 00:00:35  28
   8 2003-12-01 00:00:42  36
   9 2003-12-01 00:00:45  45

Setting ``running`` to True together with setting ``bucket_interval``
will create a "sliding windows" of the specified fixed size.
Each input tick may produce up to two buckets:

* one looks "backward" from input tick's timestamp (inclusive)
* other looks "forward" from input tick's timestamp (exclusive)

"forward" bucket is not created if the other tick has an equal "backward" bucket
or when "forward" bucket exceeds query end time.

.. doctest::

   >>> otp.run(
   ...     t.agg({'S': otp.agg.sum('A')}, running=True, bucket_interval=20)
   ... )
                     Time   S
   0  2003-12-01 00:00:00   0
   1  2003-12-01 00:00:05   1
   2  2003-12-01 00:00:13   3
   3  2003-12-01 00:00:15   6
   4  2003-12-01 00:00:20  10
   5  2003-12-01 00:00:25   9
   6  2003-12-01 00:00:27  14
   7  2003-12-01 00:00:30  20
   8  2003-12-01 00:00:33  18
   9  2003-12-01 00:00:35  22
   10 2003-12-01 00:00:40  18
   11 2003-12-01 00:00:42  26
   12 2003-12-01 00:00:45  35
   13 2003-12-01 00:00:47  30
   14 2003-12-01 00:00:50  24
   15 2003-12-01 00:00:55  17

Parameter ``all_fields``
^^^^^^^^^^^^^^^^^^^^^^^^

Parameter ``all_fields`` allows to include the other fields from the input tick in the result,
and also allows to choose which sliding windows to include in the output.

Setting ``running`` to True together with setting ``all_fields`` to True
will produce output only for "backward" sliding windows and will copy all the fields from the input tick:

.. doctest::

   >>> otp.run(
   ...     t.agg({'S': otp.agg.sum('A')}, running=True, bucket_interval=20, all_fields=True)
   ... )
                    Time  A   S
   0 2003-12-01 00:00:00  0   0
   1 2003-12-01 00:00:05  1   1
   2 2003-12-01 00:00:13  2   3
   3 2003-12-01 00:00:15  3   6
   4 2003-12-01 00:00:20  4  10
   5 2003-12-01 00:00:27  5  14
   6 2003-12-01 00:00:30  6  20
   7 2003-12-01 00:00:35  7  22
   8 2003-12-01 00:00:42  8  26
   9 2003-12-01 00:00:45  9  35

Setting ``running`` to True together with setting ``all_fields`` to `when_ticks_exit_window`
will produce output only for "forward" sliding windows and will copy all the fields from the input tick:

.. doctest::

   >>> otp.run(
   ...     t.agg({'S': otp.agg.sum('A')}, running=True, bucket_interval=20, all_fields='when_ticks_exit_window')
   ... )
                    Time  A   S
   0 2003-12-01 00:00:20  0   6
   1 2003-12-01 00:00:25  1  10
   2 2003-12-01 00:00:33  2  20
   3 2003-12-01 00:00:35  3  18
   4 2003-12-01 00:00:40  4  22
   5 2003-12-01 00:00:47  5  35
   6 2003-12-01 00:00:50  6  30
   7 2003-12-01 00:00:55  7  24

Setting ``running`` to False together with setting ``all_fields``
will copy the values of the other input fields from the first tick of each bucket:

.. doctest::

   >>> otp.run(
   ...     t.agg({'S': otp.agg.sum('A')}, bucket_interval=21, all_fields=True)
   ... )
                    Time  A   S
   0 2003-12-01 00:00:21  0  10
   1 2003-12-01 00:00:42  5  18
   2 2003-12-01 00:01:00  8  17

Examples with different buckets
-------------------------------

Find average for selected ticks for the 5 second interval with sliding window:

.. doctest::

   >>> data = otp.Ticks(
   ...     X=[10, 9, 14, 14, 8, 11],
   ...     offset=[0, 1000, 2000, 3000, 4000, 5000],
   ... )
   >>> data = data.agg({'RESULT': otp.agg.average('X')}, running=True, bucket_interval=otp.Second(5))
   >>> otp.run(data)
                     Time  RESULT
   0  2003-12-01 00:00:00   10.00
   1  2003-12-01 00:00:01    9.50
   2  2003-12-01 00:00:02   11.00
   3  2003-12-01 00:00:03   11.75
   4  2003-12-01 00:00:04   11.00
   5  2003-12-01 00:00:05   11.20
   6  2003-12-01 00:00:06   11.75
   7  2003-12-01 00:00:07   11.00
   8  2003-12-01 00:00:08    9.50
   9  2003-12-01 00:00:09   11.00
   10 2003-12-01 00:00:10     NaN

Find total volume of trades, minimal and maximum price for the first day for a symbol ``AAA``:

.. doctest::

   >>> data = otp.DataSource(db='DEMO_L1', tick_type='TRD', symbol='AAA')
   >>> data = data.agg({
   ...     'SUM': otp.agg.sum('SIZE'),
   ...     'MIN': otp.agg.min('PRICE'),
   ...     'MAX': otp.agg.max('PRICE'),
   ... }, bucket_interval=otp.Day(1))
   >>> data = data.first()
   >>> otp.run(data)
           Time   SUM    MIN    MAX
   0 2003-12-02  1600  59.72  60.24

Find an average in buckets of 5 ticks:

.. doctest::

   >>> data = otp.Ticks(X=[21, 20, 22, 25, 18, 17, 19, 23, 21, 21, 16, 20, 15])
   >>> data = data.agg({'AVG': otp.agg.average('X')}, bucket_interval=5, bucket_units='ticks')
   >>> otp.run(data)
                        Time   AVG
   0 2003-12-01 00:00:00.004  21.2
   1 2003-12-01 00:00:00.009  20.2
   2 2003-12-04 00:00:00.000  17.0


Bucket interval can be set as a *float* if ``bucket_units`` is set to *seconds*:

.. doctest::

   >>> data = otp.Ticks(X=[1, 2, 3, 4, 5, 6, 7, 8])
   >>> data = data.agg({'SUM': otp.agg.sum('X')}, bucket_interval=0.002)
   >>> otp.run(data, start=otp.config.default_start_time, end=otp.config.default_start_time + otp.Milli(20))
                        Time  SUM
   0 2003-12-01 00:00:00.002    3
   1 2003-12-01 00:00:00.004    7
   2 2003-12-01 00:00:00.006   11
   3 2003-12-01 00:00:00.008   15
   4 2003-12-01 00:00:00.010    0
   5 2003-12-01 00:00:00.012    0
   6 2003-12-01 00:00:00.014    0
   7 2003-12-01 00:00:00.016    0
   8 2003-12-01 00:00:00.018    0
   9 2003-12-01 00:00:00.020    0


Other
-----

Aggregate over :py:class:`~onetick.py.Operation` instead of :py:class:`~onetick.py.Column`:

.. doctest::

   >>> data = otp.Ticks(X=[1, 2, 3], Y=[4, 5, 6])
   >>> data = data.agg({
   ...     'SUM': otp.agg.sum(data['X'] * data['Y'])
   ... })
   >>> otp.run(data)
           Time  SUM
   0 2003-12-04   32
