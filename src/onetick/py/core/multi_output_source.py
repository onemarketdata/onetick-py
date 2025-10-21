from onetick import py as otp
from onetick.py import configuration
from onetick.py.otq import otq


class MultiOutputSource:
    """
    Construct a source object with multiple outputs
    from several connected :py:class:`~onetick.py.Source` objects.

    This object can be saved to disk as a graph using :py:meth:`~onetick.py.Source.to_otq` method,
    or passed to :py:func:`onetick.py.run` function.

    If it's passed to :py:func:`onetick.py.run`,
    then returned results for different outputs will be available as a dictionary.

    Parameters
    ----------
    outputs : dict
        Dictionary which keys are names of the output sources, and values are output sources themselves.
        All the passed sources should be connected.

    Examples
    --------

    Results for individual outputs can be accessed by output names

    >>> # OTdirective: skip-snippet:;
    >>> root = otp.Tick(A=1)
    >>> branch_1 = root.copy()
    >>> branch_2 = root.copy()
    >>> branch_3 = root.copy()
    >>> branch_1['B'] = 1
    >>> branch_2['B'] = 2
    >>> branch_3['B'] = 3
    >>> src = otp.MultiOutputSource(dict(BRANCH1=branch_1, BRANCH2=branch_2, BRANCH3=branch_3))
    >>> res = otp.run(src)
    >>> sorted(list(res.keys()))
    ['BRANCH1', 'BRANCH2', 'BRANCH3']
    >>> # OTdirective: skip-snippet:;
    >>> res['BRANCH1'][['A', 'B']]
       A  B
    0  1  1
    >>> # OTdirective: skip-snippet:;
    >>> res['BRANCH2'][['A', 'B']]
       A  B
    0  1  2
    >>> # OTdirective: skip-snippet:;
    >>> res['BRANCH3'][['A', 'B']]
       A  B
    0  1  3

    node_name parameter of the otp.run() method can be used to select outputs

    >>> # OTdirective: skip-snippet:;
    >>> src = otp.MultiOutputSource(dict(BRANCH1=branch_1, BRANCH2=branch_2, BRANCH3=branch_3))
    >>> res = otp.run(src, node_name=['BRANCH2', 'BRANCH3'])
    >>> sorted(list(res.keys()))
    ['BRANCH2', 'BRANCH3']
    >>> # OTdirective: skip-snippet:;
    >>> res['BRANCH2'][['A', 'B']]
       A  B
    0  1  2
    >>> # OTdirective: skip-snippet:;
    >>> res['BRANCH3'][['A', 'B']]
       A  B
    0  1  3

    If only one output is selected, then it's returned directly and not in a dictionary

    >>> # OTdirective: skip-snippet:;
    >>> src = otp.MultiOutputSource(dict(BRANCH1=branch_1, BRANCH2=branch_2, BRANCH3=branch_3))
    >>> res = otp.run(src, node_name='BRANCH2')
    >>> res[['A', 'B']]
       A  B
    0  1  2

    A dictionary with sources can also be passed to otp.run directly,
    and MultiOutputSource object will be constructed internally

    >>> res = otp.run(dict(BRANCH1=branch_1, BRANCH2=branch_2))
    >>> # OTdirective: skip-snippet:;
    >>> res['BRANCH1'][['A', 'B']]
       A  B
    0  1  1
    >>> # OTdirective: skip-snippet:;
    >>> res['BRANCH2'][['A', 'B']]
       A  B
    0  1  2
    """

    def __init__(self, outputs, main_branch_name=None):

        # 1. Checking that outputs have a common part:
        # we create a set of keys for all outputs and see if all sets are connected;
        # two sets are connected if they have any key in common

        if len(outputs) < 1:
            raise ValueError('At least one branch should be passed to a MultiOutputSource object')

        def get_history_key_set(hist):
            keys = set()
            for rule in hist._rules:
                if "key" in rule.key_params:
                    keys.add(rule.key)
            return keys

        source_key_sets = []
        for source in outputs.values():
            source_key_sets.append(get_history_key_set(source.node()._hist))

        while len(source_key_sets) > 1:
            # we take first set from the list and add to it all the other sets that have common keys with it
            # we continue to do this until first set is the only set in the list or until it has no common keys
            # with other sets in the list
            new_key_sets = []
            first_key_set = source_key_sets[0]
            new_key_sets.append(first_key_set)
            for s in source_key_sets[1:]:
                if first_key_set.isdisjoint(s):
                    # no common keys
                    new_key_sets.append(s)
                else:
                    # there are common keys
                    first_key_set = first_key_set | s
            # checking if first_key_set had common keys with at least some other set
            if len(source_key_sets) == len(new_key_sets):
                raise ValueError("Cannot construct a MultiOutputSource object from outputs that are not connected!")
            # moving first_key_set to the end; maybe it will make things work faster
            new_key_sets = new_key_sets[1:] + [first_key_set]
            source_key_sets = new_key_sets

        # 2, 3. Assigning node names and selecting main branch
        self.__main_branch_name = None
        self.__main_branch = None
        self.__side_branches = {}
        for node_name, source in outputs.items():
            source = source.copy()
            # this is necessary to create different branches if a source is a branching point
            source.sink(otq.Passthrough())
            source.node().node_name(node_name)
            if self.__main_branch_name is None and (main_branch_name is None or main_branch_name == node_name):
                self.__main_branch_name = node_name
                self.__main_branch = source
            else:
                self.__side_branches[node_name] = source
        if self.__main_branch_name is None:
            raise ValueError(f'Branch name "{main_branch_name}" not found among passed outputs!')

        # 4, 5. Apply other branches to the main branch and copy dicts
        self.__main_branch._apply_side_branches(self.__side_branches.values())

    def _all_node_names(self):
        return [self.__main_branch_name] + list(self.__side_branches.keys())

    def _side_branch_list(self):
        return list(self.__side_branches.values())

    def get_branch(self, branch_name: str) -> otp.Source:
        """
        Retrieve a branch by its name.

        Parameters
        ----------
        branch_name : str
            The name of the branch to retrieve.

        Returns
        -------
        otp.Source
            The branch corresponding to the given name.
        """
        if branch_name == self.__main_branch_name:
            return self.__main_branch

        branch = self.__side_branches.get(branch_name)
        if branch is None:
            raise ValueError(f'Branch name "{branch_name}" not found among the outputs!')

        return branch

    @property
    def main_branch(self) -> otp.Source:
        """
        Get the main branch.

        Returns
        -------
        otp.Source
            The main branch.
        """
        return self.__main_branch

    def _prepare_for_execution(self, symbols=None, start=None, end=None, start_time_expression=None,
                               end_time_expression=None, timezone=None,
                               has_output=None,  # NOSONAR
                               running_query_flag=None, require_dict=False, node_name=None,
                               symbol_date=None):

        has_output = False  # to avoid sinking PASSTHROUGH to the main branch
        if node_name is None:  # if user passed a node name, we shouldn't overwrite it
            node_name = self._all_node_names()
        return self.__main_branch._prepare_for_execution(
            symbols=symbols, start=start, end=end, start_time_expression=start_time_expression,
            end_time_expression=end_time_expression, timezone=timezone, has_output=has_output,
            running_query_flag=running_query_flag, require_dict=require_dict,
            node_name=node_name,
            symbol_date=symbol_date,
        )

    def to_otq(self, file_name=None, file_suffix=None, query_name=None, symbols=None, start=None, end=None,
               timezone=None):
        """
        Constructs an onetick query graph and saves it to disk

        See Also
        --------
        :py:meth:`onetick.py.Source.to_otq`
        """
        if timezone is None:
            timezone = configuration.config.tz
        return self.__main_branch.to_otq(file_name=file_name,
                                         file_suffix=file_suffix,
                                         query_name=query_name,
                                         symbols=symbols,
                                         start=start,
                                         end=end,
                                         timezone=timezone,
                                         add_passthrough=False)

    def _store_in_tmp_otq(self, *args, **kwargs):
        return self.main_branch._store_in_tmp_otq(*args, **kwargs)
