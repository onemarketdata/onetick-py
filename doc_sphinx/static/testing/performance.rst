.. _performance:

Performance measurement
=======================

``onetick.py`` and OneTick provide some tools and classes to run the query and measure its performance.

measure_perf.exe
----------------

Function :func:`onetick.py.perf.measure_perf` can be used to call OneTick's **measure_perf.exe** tool.

It runs specified query and prints the summary in the specified file.

Parsing summary
---------------

Classes :class:`onetick.py.perf.PerformanceSummaryFile` and :class:`onetick.py.perf.MeasurePerformance`
can be used to parse contents of the summary file to python format.

Additionally, configuration parameter :py:attr:`stack_info<onetick.py.configuration.Config.stack_info>`
can be used to add some python debug information to the parsed result.

Session metrics
---------------

You can also gather performance metrics for all queries made in a session.
To do this, set ``gather_performance_metrics`` parameter to ``True``
when creating an :class:`otp.Session <onetick.py.Session>` object.

Metrics could be accessed after session close via ``otp.Session.performance_metrics`` property.

Example of collecting performance metrics, returned data format and information about limitations you can find in the
:class:`otp.Session <onetick.py.Session>` documentation.
