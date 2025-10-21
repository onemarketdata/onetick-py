# Overview

``onetick.py`` is a versatile and efficient Python library designed for handling tick data with ease.
It harnesses the power of OneTick, the industry-leading tick analytics technology, to process tick
data at unparalleled speeds. This library is tailored both for existing OneTick users,
as a Python interface into OneTick, and for new users who seek an intuitive tool for tick data analysis
that comes with tick data from 200+ exchanges.

The primary strength of ``onetick.py`` lies in its similarity to the popular Python library, Pandas.
Users familiar with Pandas find ``onetick.py`` easy to learn. In particular, ``onetick.py`` users
can think in terms of Python expressions, built-ins, and native math operations on tick fields.

``onetick.py`` goes far beyond the functionality that exists in Pandas exposing all of the power of OneTick
(decades of development for the capital markets use cases) in a Pandas-like style. Crucially, the
Pandas-like syntax is translated into the OneTick query language, executing on the high-performance
OneTick tick server engine. In a nutshell, ``onetick.py`` combines the ease of use of Python
with the performance of  OneTick.

## Installation

The latest version of ``onetick-py`` is available on PyPI: <https://pypi.org/project/onetick-py/>.

    pip install onetick-py[webapi]

Use ``webapi`` extra to easily use it with remote OneTick REST Servers, such as [OneTick Cloud](https://www.onetick.com/cloud-services).

See [Getting Started](https://docs.pip.distribution.sol.onetick.com/static/getting_started/root.html)
section in the documentation to see how quickly set up ``onetick-py`` configuration
and authentication and start running queries.

For other installation options, including using ``onetick-py`` with locally installed OneTick server,
see [Installation](https://docs.pip.distribution.sol.onetick.com/static/installation/webapi.html)
section in the documentation.

## Key Features

- **Pandas-like API**: Familiar syntax and functions for those accustomed to Pandas.
- **High-Performance**: Executes complex queries rapidly using the underlying OneTick C++ engine.
- **Real-time processing / CEP**: Same intuitive syntax for real-time and historical analytics.
- **Convenient Data Inspection and Testing**: Integrated tools for debugging, data inspection, and testing with pytest.
- **Enterprise-Grade Features**: Includes support for authentication, access control, encryption, and entitlements.
- **Comprehensive Documentation**: Public multiversion documentation with examples, guides, and use cases.

## Applications

- **TCA / BestEx**: Quickly implement your own TCA / BestEx analytics.
- **Quant research**: Ideal for complex, high-performance tick data analysis.
- **Algorithmic Trading**: Efficient for developing and testing trading algorithms.
- **Data Visualization**: Compatible with OneTick's visualization tool, OneTick Dashboard.
- **Machine Learning**: Integrates with the OneTick ML library [onetick-ml](https://dsframework.pip.distribution.sol.onetick.com/intro.html).
- **Industry Applications**: Industry leading OneTick's Trade Surveillance and BestEx/TCA solutions are written in ``onetick.py``.
- **Back Testing**: Retrieve historic market data and metrics.
- **Market Microstructure**:  Consolidated Book Depth analysis.

## Advantages Over Competitors

- **Ease of Use**: The only language that provides the performance of a DSL without having to learn a new syntax.
- **Performance**: Demonstrates superior performance and memory efficiency compared to similar tools.
- **Parallel Processing**: Natively parallelizable by security and by day.
- **Production-Ready**: Ideal for debugging, testing, and CI/CD.

## Ways to use ``onetick.py``

``onetick.py`` can be used to analyze data managed and hosted by OneTick or managed and
hosted by the customer in a local OneTick installation.

### Hosted OneTick

- Hosted OneTick comes with high quality tick data, daily data, and reference data for 200+ global markets.
- OneTick offers T+1 and real-time tick data that is normalized and quality checked.
- OneTick supports a variety of symbologies and handles corporate action adjustments.
- The installation is a simple ``pip install`` (details [here](https://docs.pip.distribution.sol.onetick.com/static/installation/webapi.html)).

### Local OneTick

- Manage all of your market data and order flow in the industry-leading tick management platform OneTick.
- Have your users access and analyze the data via a simple and intuitive ``onetick.py`` API.
- Managed services options are available.
- Installation details are [here](https://docs.pip.distribution.sol.onetick.com/static/installation/onprem.html).

## OneTick

``onetick-py`` is part of a bigger tick management OneTick ecosystem. You may want to turn to OneTick for

- Visual query designer.
- Collecting, loading, and storing tick data.
- Other APIs including C++, C#, Java, R, Matlab.
- Learn more at [onetick.com/onetick-tick-analytics-platform](https://www.onetick.com/onetick-tick-analytics-platform).
