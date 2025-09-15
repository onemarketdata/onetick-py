from abc import ABCMeta, abstractmethod
from onetick.py.otq import otq
from onetick.py.backports import singledispatchmethod


class _BaseCutBuilder(metaclass=ABCMeta):
    """We need to build up query graph to compute bins based on max and min value.
    As we need to set proper output column name in per-tick script:
    1) instance of this class created on cut() and qcut() call,
    2) and then this instance is called during _Source.__setattr__() with output_column_name as argument.
    """

    def __init__(self, src, column, bins, **kwargs):
        self.src = src
        self.column = column
        self.bins = bins
        self.column_name = str(self.column)
        self.output_column_name = None
        self.labels = kwargs.get('labels')
        self.res = None
        self.bin_number = None
        if isinstance(self.bins, (int, float)) and self.labels and len(self.labels) != self.bins:
            raise ValueError('Number of labels is not equal to number of bins')

    def __call__(self, output_column_name):
        self.output_column_name = output_column_name
        self.build_graph()

    def build_graph(self):
        res = self.define_state_variables(self.bins)
        script = self.generate_script(res)
        self.src.script(script, inplace=True)

    @abstractmethod
    def compute_bin_variables(self):
        """Compute state variables based on min/max and number of bins/quantiles."""
        pass

    @singledispatchmethod
    def define_state_variables(self, bins):
        raise NotImplementedError(f'Method define_state_variables() is not implemented for parameter {type(bins)}')

    @define_state_variables.register
    def dsv_int(self, bins: int):
        res = self.compute_bin_variables()

        # merge src with bins res (to share state_vars)
        res.schema.set(**{})
        res, _ = res[(res['Time'] == 0)]
        self.src.sink(otq.Merge(identify_input_ts=False))
        self.src.source(res.node().copy_graph())
        self.src.node().add_rules(res.node().copy_rules())
        return res

    @define_state_variables.register
    def dsv_list(self, bins: list):
        return self.define_state_variables_by_list(bins)

    def define_state_variables_by_list(self, bins):
        raise NotImplementedError("define_state_variables_by_list() not implemented")

    def state_variable(self, inx):
        """State variable name used in bin's calculations"""
        return f'_TMP_{self.output_column_name}_{self.column_name}_{inx}'

    def generate_script(self, res):
        """
        Per-tick script generator.
        Every tick is compared state variable to find index of bin.
        Resulted bin (interval or label) is saved in output column.
        """
        bin_number = self.bin_number
        labels = self.labels
        column_name = self.column_name
        output_column_name = self.output_column_name

        s = ''
        for inx in range(1, bin_number):
            s += f'if ({column_name} <= STATE::{self.state_variable(inx)})' + '{\n'
            if labels is None:
                str_val = ''
                if inx > 1:
                    str_val = '"(" + ' + f'tostring(STATE::{self.state_variable(inx - 1)}, 10, 10) + '
                str_val += '", " + ' + f'tostring(STATE::{self.state_variable(inx)}, 10, 10)' + ' + "]"'
                s += f'{output_column_name} = ' + str_val + ';\n'
            else:
                s += f'{output_column_name} = "{labels[inx - 1]}";\n'

            s += '}'
            if inx < bin_number - 1:
                s += ' else '
            else:
                s += '\n'

        # default output column value is the last bin
        if labels is None:
            var_name = self.state_variable(bin_number - 1)
            self.src[output_column_name] = '(' + res.state_vars[var_name].apply(str) + ','
        else:
            self.src[output_column_name] = labels[-1]

        return s


# pylint: disable-next=abstract-method
class _CutBuilder(_BaseCutBuilder):

    def compute_bin_variables(self):
        """Compute state variables based on min/max and number of bins."""
        res = self.src.copy()
        column_name = self.column_name
        self.bin_number = self.bins

        res.sink(otq.Compute(
            compute=f"HIGH(INPUT_FIELD_NAME='{column_name}') HV,LOW(INPUT_FIELD_NAME='{column_name}') LV",
            append_output_field_name=False))
        res.schema.set(**{"HV": self.column.dtype, "LV": self.column.dtype})

        for inx in range(self.bins):
            res.state_vars[self.state_variable(inx)] = 0.
            res.state_vars[self.state_variable(inx)] = res['LV'] + (res['HV'] - res['LV']) / self.bins * inx
        return res

    def define_state_variables_by_list(self, bins: list):
        res = self.src
        self.bin_number = len(bins)
        for inx, value in enumerate(bins):
            res.state_vars[self.state_variable(inx)] = float(value)
        return res


class _QCutBuilder(_BaseCutBuilder):

    def compute_bin_variables(self):
        """Compute state variables based on otq.Percentile()."""
        res = self.src.copy()
        column_name = self.column_name
        self.bin_number = self.bins

        res.sink(otq.Percentile(number_of_quantiles=self.bins, input_field_names=column_name))
        res.sink(otq.Transpose(direction='ROWS_TO_COLUMNS', key_constraint_values=self.bins - 1))
        res.schema.set(**{column_name + f'_{inx}': self.column.dtype for inx in range(1, self.bins)})

        for inx in range(1, self.bins):
            res.state_vars[self.state_variable(inx)] = .0
            res.state_vars[self.state_variable(inx)] = res[f'{column_name}_{inx}']
        return res

    def define_state_variables_by_list(self, bins: list):
        raise NotImplementedError("qcut() with q as list of float is not implemented yet.")
