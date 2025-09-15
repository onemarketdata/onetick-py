import io
import os
import subprocess
import warnings
import dataclasses
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Union, Type, Tuple

import pandas as pd
import onetick.py as otp
from onetick.py.otq import otq
from onetick.py.backports import cache
from onetick.py.core import query_inspector
from . import adaptive


if otp.__webapi__ or os.getenv("OTP_SKIP_OTQ_VALIDATION"):
    MEASURE_PERF = None
else:
    # see http://solutions.pages.soltest.onetick.com/iac/onetick-server/MeasurePerf.html
    MEASURE_PERF = Path(otp.__one_tick_bin_dir__) / 'measure_perf.exe'


@cache
def _get_allocation_lib() -> Optional[str]:
    suffix = '.dll' if os.name == 'nt' else '.so'
    allocation_lib = Path(otp.__one_tick_bin_dir__) / ('liballocation_interceptors' + suffix)
    if not allocation_lib.exists():
        warnings.warn(f"Can't find file {allocation_lib}, memory statistics will not be calculated")
        return None
    if os.name == 'nt':
        withdll_exe = Path(otp.__one_tick_bin_dir__) / 'withdll.exe'
        if not withdll_exe.exists():
            warnings.warn(f"Can't find file {withdll_exe}, memory statistics will not be calculated")
            return None
        return f'{withdll_exe} -d:{allocation_lib}'
    else:
        return f'LD_PRELOAD={allocation_lib}'


def _run_measure_perf(otq_file: str, summary_file: str, context: Optional[str] = None):
    if otp.__webapi__:
        raise RuntimeError("Can't use measure_perf.exe in WebAPI mode.")
    if MEASURE_PERF is not None and not MEASURE_PERF.exists():  # noqa
        raise RuntimeError(f"File {MEASURE_PERF} doesn't exist, can't execute it.")
    allocation_lib = _get_allocation_lib()
    cmd = allocation_lib or ''
    if cmd:
        cmd += ' '
    cmd += f'{MEASURE_PERF} -otq_file {otq_file} -summary_file {summary_file}'
    if context:
        cmd += f' -context {context}'
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError:
        if os.name == 'nt' and allocation_lib and Path(str(summary_file)).exists():
            # withdll.exe on Windows returns strange exit codes, but runs successfully anyway
            return
        raise


def measure_perf(src_or_otq: Union['otp.Source', str],
                 summary_file: Union[str, 'otp.utils.TmpFile', None] = None,
                 context: Union[str, Type[adaptive], None] = adaptive) -> Tuple[str, Union[str, 'otp.utils.TmpFile']]:
    """
    Run **measure_perf.exe** tool on some .otq file or :py:class:`onetick.py.Source`.
    Result is saved in file ``summary_file``.
    If it is not set, then temporary :py:class:`onetick.py.utils.temp.TmpFile` is generated and returned.

    Parameters
    ----------
    src_or_otq: :py:class:`~onetick.py.Source` or str
            :py:class:`~onetick.py.Source` object or path to already existing .otq file.
    summary_file: str
        path to the resulting summary file.
        By default some temporary file name will be used.
    context: str
        context that will be used to run the query.

    Returns
    -------
    Returns tuple with the path to the generated query and path to the summary file.

    Examples
    --------
    >>> t = otp.Tick(A=1)
    >>> otq_file, summary_file = otp.perf.measure_perf(t)
    >>> with open(summary_file) as f:  # doctest: +ELLIPSIS
    ...    print(f.read())
    Running result of ...
    ...
    index,EP_name,tag,...
    ...
    """
    if isinstance(src_or_otq, otp.Source):
        otq_file = src_or_otq.to_otq()
    else:
        otq_file = src_or_otq
    if not summary_file:
        summary_file = otp.utils.TmpFile()
    context_str = otp.config.context if context is adaptive else context
    _run_measure_perf(otq_file, str(summary_file), context_str)  # type: ignore
    return otq_file, summary_file


@dataclass
class SummaryEntry:
    def __getitem__(self, item):
        """
        Get value of the entry field by name.
        """
        return getattr(self, item)

    def __setitem__(self, item, value):
        """
        Set value of the entry field by name.
        """
        return setattr(self, item, value)

    def asdict(self) -> dict:
        """
        Return entry as a dictionary of field names and their values.
        """
        return dataclasses.asdict(self)

    def __iter__(self):
        """
        Iterator that returns tuples with name and value of each field.
        """
        for k, v in self.asdict().items():
            yield k, v

    @classmethod
    @cache
    def fields(cls):
        """
        Get list of entries field objects.
        """
        return dataclasses.fields(cls)

    @classmethod
    @cache
    def field_names(cls):
        """
        Get list of entries field names.
        """
        return [field.name for field in cls.fields()]


@dataclass
class DebugSummaryEntry:
    #: internal stack info number to identify debug information
    stack_info: Optional[str] = None
    #: python traceback string to identify location of the python code that created OneTick's EP
    traceback: Optional[str] = None


@dataclass
class _OrdinarySummaryEntry(SummaryEntry):
    #: Sequential number of the EP in the report
    index: int
    #: Name of the EP
    EP_name: str
    #: EP full tag (scope will be added to the tag if there is any)
    tag: int
    #: Time elapsed for EP execution with its child nodes in microseconds
    running_time_with_children: int
    #: Individual time elapsed for EP execution in microseconds
    running_time: int
    #: Number of ticks processed by the EP
    processed_tick_events: int
    #: Number of tick descriptors processed by the EP
    processed_schema_events: int
    processed_timer_events: int
    #: Maximal number of ticks accumulated by the EP during query execution
    #: This field is calculated only for aggregations (for example, EPs with a sliding window or GROUP_BY).
    #: For all other EPs, it has the value of 0.
    max_accumulated_ticks_count: int
    #: For continuous queries, each EP measures the latency in microseconds for all the ticks it has propagated.
    #: The latency of a tick is considered to be the difference between
    #: tick propagation host time and the timestamp of the tick.
    #: The maximum value of this latency (calculated by the EP)
    #: is reported by measure_perf.exe in the summary of that EP.
    #:
    #: The latency is calculated neither for aggregations with BUCKET_TIME=BUCKET_START
    #: (as ticks are propagated by overwritten timestamps that are equal to the bucket start) nor for their child EPs.
    #: For such cases, the following max_introduced_latency
    #: special values indicate the reason why the maximum introduced latency was not calculated:
    #:
    #: * -3 indicates that the EP is the culprit for latency calculation interruption
    #: * -2 indicates that the latency calculation for the EP is turned off because
    #:   its source EP's max_introduced_latency is -3
    #: * -1 indicates that the query is non-continuous
    max_introduced_latency: int
    #: There are EPs (like PRESORT, Aggregations, and others)
    #: that are allowed to propagate received ticks with some delay.
    #: This flag indicates if the EP introduces delay.
    ep_introduces_delay_flag: int
    #: The amount of memory allocated by EP and its child nodes.
    allocated_memory_with_children: int
    #: The amount of memory allocated by EP.
    allocated_memory: int
    #: The amount of memory unreleased by EP. The usual cause of non-zero unreleased memory is EP's cached data.
    unreleased_memory_with_children: int
    #: The amount of memory unreleased by EP and its child nodes.
    #: The usual cause of non-zero unreleased memory is EP's and its child nodes' cached data.
    unreleased_memory: int
    #: Peak memory utilization introduced by EP and its child nodes.
    peak_allocated_memory: int


@dataclass
class OrdinarySummaryEntry(DebugSummaryEntry, _OrdinarySummaryEntry):
    """
    Data class for each line of ordinary performance summary.
    """
    pass


@dataclass
class _PresortSummaryEntry(SummaryEntry):
    #: Sequential number of the branch in PRESORT EPs summary section
    index: int
    #: Source EP name of combined PRESORT EP source branch for which the summary was reported
    presort_source_ep_name: str
    #: Combined PRESORT EP name
    presort_sink_ep_name: str
    #: Source EP tag of combined PRESORT EP source branch for which the summary was reported
    presort_source_ep_tag: int
    #: Combined PRESORT EP tag
    presort_sink_ep_tag: int
    #: Maximum accumulated ticks count by PRESORT for the located branch.
    max_accumulated_ticks_count: int


@dataclass
class PresortSummaryEntry(DebugSummaryEntry, _PresortSummaryEntry):
    """
    Data class for each line of PRESORT performance summary.
    """
    pass


@dataclass
class _CEPSummaryEntry(SummaryEntry):
    #: Sequential number of the root EP in the root EPs summary section
    index: int
    #: Root EP name for which summary is provided
    sink_ep_name: str
    #: Root EP tag for which summary is provided
    sink_ep_tag: int
    #: Mean of the latencies of all ticks passed through the node
    latencies_mean: float
    #: Standard deviation of the latencies of all ticks passed through the node
    latencies_standard_deviation: float
    #: Average slope of the linear regression function found by least squares method calculated for all latencies
    #: of all ticks passed through the root node.
    #: As mentioned earlier, the regression function can be considered as a function describing some relationship
    #: between two variables: tick latency and tick arrival timestamp.
    latencies_average_slope: float
    #: This is the average variance of ticks latencies from the computed linear regression function.
    latencies_variance_from_regression_line: float


@dataclass
class CEPSummaryEntry(DebugSummaryEntry, _CEPSummaryEntry):
    """
    Data class for each line of CEP performance summary.
    """
    pass


class PerformanceSummary:
    _entry_cls: Optional[Type[SummaryEntry]] = None
    _entry_key: Optional[str] = None
    _ep_name_field: Optional[str] = None

    def __init__(self, text: Optional[str]):
        #: text of the summary (csv format)
        self.text = text
        #: pandas.DataFrame from the data of the summary
        self.dataframe = pd.read_csv(io.StringIO(self.text)) if self.text else pd.DataFrame()
        #: list of corresponding entries objects
        self.entries = self.dataframe.to_dict('records')
        #: mapping of EP tags to corresponding entry objects
        self.entries_dict = {}
        if self._entry_cls is not None:
            self.entries = [self._entry_cls(**e) for e in self.entries]
        if self._entry_key is not None:
            self.entries_dict = {e[self._entry_key]: e for e in self.entries}

    def __iter__(self):
        """
        Iterate over list of summary :attr:`entries`.
        """
        yield from self.entries


class OrdinarySummary(PerformanceSummary):
    """
    This is the first section in the summary file containing the largest portion of the summary for graph nodes.
    """

    _entry_cls = OrdinarySummaryEntry
    _entry_key = 'tag'
    _ep_name_field = 'EP_name'


class PresortSummary(PerformanceSummary):
    """
    In PRESORT EPs summary section **measure_perf.exe** provides per PRESORT source branch report
    containing max accumulated ticks count by PRESORT for each of these branches.
    Namely, it shows how many ticks were accumulated by PRESORT for each of these source branches.

    Please note that there are some PRESORT EP types, like SYNCHRONIZE_TIME EP,
    that do not support performance measurement, yet.

    Each line of this section contains six fields
    representing the location of the branch for which the report is printed
    and a field that contains the maximum number of ticks accumulated by PRESORT for this branch.

    The location of a branch is determined by the source and sink EP names and tags.
    """

    _entry_cls = PresortSummaryEntry
    _entry_key = 'presort_sink_ep_tag'
    _ep_name_field = 'presort_sink_ep_name'


class CEPSummary(PerformanceSummary):
    """
    The last summary type produced by **measure_perf.exe**
    is the latency summary for root EPs of the executed top-level query in CEP mode.

    Each root EP in CEP mode measures tick arrival latency before processing and propagating it to the sinks,
    down by the graph.

    Note that for non-CEP mode this summary is not printed at all.

    The summary provided in this section tries to shed some light
    and estimate the relationship between the following two variables:

    * dependent variable - tick latency
    * independent variable - tick arrival time into the root node.

    The summary printed in this section tries to describe this relationship using some statistical analysis metrics.

    Please note that these values are calculated across all ticks in all symbols processed by the query.

    Calculated stats for ROOT EPs are printed once the query is finished and there are no more ticks left to arrive.

    This summary contains the mean of latencies, standard deviation,
    average slope of linear regression function (calculated by the least squares method),
    and average variance from the regression function computed based on latency numbers of ticks
    that are passed through each root EP of a top-level query.

    For each root node, one line is printed with the fields containing values for each of the above-mentioned metrics.
    This summary should be enough to determine slow consumer queries and try to debug and optimize those.
    """

    _entry_cls = CEPSummaryEntry
    _entry_key = 'sink_ep_tag'
    _ep_name_field = 'sink_ep_name'


class PerformanceSummaryFile:
    def __init__(self, summary_file: Union[str, os.PathLike]):
        """
        Class to read and parse ``summary_file`` that was generated by OneTick's measure_perf.exe

        Parsed result is accessible via public properties of the class.

        Parameters
        ----------
        summary_file:
            path to the summary file.

        Examples
        --------
        >>> t = otp.Tick(A=1)
        >>> otq_file, summary_file = otp.perf.measure_perf(t)
        >>> result = otp.perf.PerformanceSummaryFile(summary_file)
        >>> print(result.ordinary_summary.dataframe)  # doctest: +ELLIPSIS
        index      EP_name  tag ...
            0  PASSTHROUGH    0 ...
        ...
        """
        #: path to the summary file
        self.summary_file = Path(summary_file)
        #: the text of the summary file
        self.summary_text = self.summary_file.read_text()
        ordinary_summary, presort_summary, cep_summary = self._parse()
        #: :class:`Ordinary summary <onetick.py.perf.OrdinarySummary>`
        self.ordinary_summary = ordinary_summary
        #: :class:`Presort summary <onetick.py.perf.PresortSummary>`
        self.presort_summary = presort_summary
        #: :class:`CEP summary <onetick.py.perf.CEPSummary>`
        self.cep_summary = cep_summary

    def _parse(self):
        summary_text_lines = self.summary_text.splitlines(keepends=True)

        summaries = {}
        for i, line in enumerate(summary_text_lines):
            if not line.startswith('index,'):
                continue
            header = set(line.strip().split(','))
            for summary_cls in (OrdinarySummary, PresortSummary, CEPSummary):
                if header.issubset(summary_cls._entry_cls.field_names()):
                    break
            else:
                raise ValueError("Can't parse performance summary")
            summaries[summary_cls] = i

        tables_header_line_indexes = list(summaries.values())
        for i, (summary_cls, start) in enumerate(summaries.items()):
            if i + 1 < len(tables_header_line_indexes):
                end = tables_header_line_indexes[i + 1]
            else:
                end = None
            summary_table_text = ''.join(summary_text_lines[start:end])
            summaries[summary_cls] = summary_cls(summary_table_text)

        return tuple(
            summaries.get(summary_cls) or summary_cls(None)
            for summary_cls in (OrdinarySummary, PresortSummary, CEPSummary)
        )


def _get_query_nodes(otq_file: str):
    """
    Get query nodes ids and stack info.
    """
    otq_file, _, query_name = otq_file.partition('::')
    info = query_inspector.get_query_info(otq_file, query_name)
    result = {}
    for node_tag, node in info.nodes.items():
        ep_name, stack_info = node._get_ep_name_and_stack_info()
        result[node_tag] = {'node_tag': node_tag, 'ep_name': ep_name, 'stack_info': stack_info}
    return result


class MeasurePerformance(PerformanceSummaryFile):
    def __init__(self, src_or_otq, summary_file=None, context=adaptive):
        """
        Class to run OneTick's measure_perf.exe on the specified query and parse the result.

        Additionally some debug information about the python location of event processor objects
        may be added to the result if
        :py:attr:`stack_info<onetick.py.configuration.Config.stack_info>`
        configuration parameter is set.

        Parsed result is accessible via public properties of the class.

        Parameters
        ----------
        src_or_otq: :py:class:`~onetick.py.Source` or str
            :py:class:`~onetick.py.Source` object or path to already existing .otq file.
        summary_file: str
            path to the resulting summary file.
            By default some temporary file name will be used.
        context: str
            context that will be used to run the query.

        Examples
        --------
        >>> t = otp.Tick(A=1)
        >>> result = otp.perf.MeasurePerformance(t)
        >>> print(result.ordinary_summary.dataframe)  # doctest: +ELLIPSIS
        index      EP_name  tag ...
            0  PASSTHROUGH    0 ...
        ...
        """

        self.otq_file, summary_file = measure_perf(src_or_otq, summary_file, context)
        super().__init__(summary_file)
        self._query_nodes = _get_query_nodes(self.otq_file)
        for summary in (self.ordinary_summary, self.presort_summary, self.cep_summary):
            self._add_debug_info_to_summary(summary)

    def _add_debug_info_to_summary(self, summary):
        """
        Modify entries in ``summary`` to include **stack_info** and **traceback** parameters.
        """
        from onetick.py._stack_info import _get_traceback_with_id
        for tag, entry in summary.entries_dict.items():
            if tag not in self._query_nodes:
                warnings.warn(f"Can't find node with tag {tag} in file {self.otq_file}")
                continue
            debug_info = self._query_nodes[tag]
            ep_name = entry[summary._ep_name_field]
            assert ep_name.split('/')[0] == debug_info['ep_name']
            stack_info_uuid = debug_info['stack_info']
            if stack_info_uuid:
                traceback = _get_traceback_with_id(stack_info_uuid)
                entry['stack_info'] = stack_info_uuid
                entry['traceback'] = traceback
