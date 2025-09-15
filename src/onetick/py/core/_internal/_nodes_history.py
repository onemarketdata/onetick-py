from abc import ABC, abstractmethod
from typing import List
from copy import deepcopy


class _NodeRule(ABC):
    key_params: List[str] = []

    @abstractmethod
    def build(self, eps, print_out=False):
        raise NotImplementedError

    def __repr__(self):
        return f'{self.__class__.__name__}({id(self)})'


class _Assign(_NodeRule):
    key_params = ['key']

    def __init__(self, ep, key):
        self.ep = ep
        self.key = key

    def build(self, eps, print_out=False):
        if self.key not in eps:
            eps[self.key] = self.ep.copy()

        if print_out:
            print(f'[API] Create "{str(eps[self.key]).strip()}" (id={id(eps[self.key])})')

        return eps[self.key]


class _TickType(_NodeRule):
    key_params = ['key']

    def __init__(self, ep, key, tt):
        self.ep = ep
        self.key = key
        self.tt = tt

    def build(self, eps, print_out=False):
        if self.key not in eps:
            eps[self.key] = self.ep.copy()

        if print_out:
            print(f'[API] Set tick type "{self.tt}" for "{str(eps[self.key]).strip()}" (id={id(eps[self.key])})')

        eps[self.key].tick_type(self.tt)
        return eps[self.key]


class _Symbol(_NodeRule):
    key_params = ['key']

    def __init__(self, ep, key, symbol):
        self.ep = ep
        self.key = key
        self.symbol = symbol

    def build(self, eps, print_out=False):
        if self.key not in eps:
            eps[self.key] = self.ep.copy()

        if print_out:
            print(f'[API] Set symbol "{self.symbol}" for "{str(eps[self.key]).strip()}" (id={id(eps[self.key])})')

        eps[self.key].set_symbol(self.symbol)
        return eps[self.key]


class _NodeName(_NodeRule):
    key_params = ['key']

    def __init__(self, key, name):
        self.key = key
        self.name = name

    def build(self, eps, print_out=False):
        eps[self.key].node_name(self.name)

        if print_out:
            print(f'[API] Set name "{self.name}" for "{str(eps[self.key]).strip()}" (id={id(eps[self.key])})')

        return eps[self.key]


class _Sink(_NodeRule):
    key_params = ['p_key', 'key']

    def __init__(self, p_ep, p_key, p_out_pin, ep, key, in_pin):
        self.p_ep = p_ep
        self.p_key = p_key
        self.p_out_pin = p_out_pin
        self.ep = ep
        self.key = key
        self.in_pin = in_pin

    def build(self, eps, print_out=False):
        if self.p_key not in eps:
            eps[self.p_key] = self.p_ep.copy()
        if self.key not in eps:
            eps[self.key] = self.ep.copy()

        t_p_ep = eps[self.p_key]
        t_ep = eps[self.key]

        if self.p_out_pin:
            t_p_ep = eps[self.p_key][self.p_out_pin]
        if self.in_pin:
            t_ep = eps[self.key](self.in_pin)

        if print_out:
            print(f'[API] Connect "{str(t_p_ep).strip()}"[out_pin={self.p_out_pin}] -> '
                  f'"{str(t_ep).strip()}"[self.in_pin={self.in_pin}] (id={id(t_p_ep)} and id={id(t_ep)})')

        t_p_ep.sink(t_ep)
        return t_ep


class _Source(_NodeRule):
    key_params = ['p_key', 'key']

    def __init__(self, p_ep, p_key, p_in_pin, ep, key, out_pin):
        self.p_ep = p_ep
        self.p_key = p_key
        self.p_in_pin = p_in_pin
        self.ep = ep
        self.key = key
        self.out_pin = out_pin

    def build(self, eps, print_out=False):
        if self.p_key not in eps:
            eps[self.p_key] = self.p_ep.copy()
        if self.key not in eps:
            eps[self.key] = self.ep.copy()

        t_p_ep = eps[self.p_key]
        t_ep = eps[self.key]

        if self.p_in_pin:
            t_p_ep = eps[self.p_key](self.p_in_pin)
        if self.out_pin:
            t_ep = eps[self.key][self.out_pin]

        if print_out:
            print(f'[API] Connect "{str(t_ep).strip()}"[out_pin={self.out_pin}] -> '
                  f'"{str(t_p_ep).strip()}"[in_pin={self.p_in_pin}] (id={id(t_ep)} and id={id(t_p_ep)})')

        t_p_ep.source(t_ep)
        return t_p_ep


class _SourceByKey(_NodeRule):
    key_params = ['p_key', 'key']

    def __init__(self, p_key, ep, key, out_pin):
        self.p_key = p_key
        self.ep = ep
        self.key = key
        self.out_pin = out_pin

    def build(self, eps, print_out=False):
        if self.key not in eps:
            eps[self.key] = self.ep.copy()

        t_p_ep = eps[self.p_key]
        t_ep = eps[self.key]

        if self.out_pin:
            t_ep = eps[self.key][self.out_pin]

        if print_out:
            print(f'[API] Connect "{str(t_ep).strip()}" -> "{str(t_p_ep).strip()}" (id={id(t_ep)} and id={id(t_p_ep)})')

        t_p_ep.source(t_ep)
        return t_p_ep


class _NodesHistory:
    def __init__(self):
        self._rules: List[_NodeRule] = []

    def assign(self, ep, key):
        self._rules.append(_Assign(ep, key))

    def tick_type(self, ep, key, tt):
        self._rules.append(_TickType(ep, key, tt))

    def symbol(self, ep, key, symbol):
        self._rules.append(_Symbol(ep, key, symbol))

    def node_name(self, key, name):
        self._rules.append(_NodeName(key, name))

    def sink(self, p_ep, p_key, p_out_pin, ep, key, in_pin):
        self._rules.append(_Sink(p_ep, p_key, p_out_pin, ep, key, in_pin))

    def source(self, p_ep, p_key, p_in_pin, ep, key, out_pin):
        self._rules.append(_Source(p_ep, p_key, p_in_pin, ep, key, out_pin))

    def source_by_key(self, p_key, ep, key, out_pin):
        self._rules.append(_SourceByKey(p_key, ep, key, out_pin))

    def rebuild(self, keys: dict):
        """Rebuild history, change uuid for each node"""
        processed = set()
        for rule in self._rules:
            if id(rule) in processed:
                # this can happen when we are merging the branch of the source with this source.
                # In this case some rules in the source and the branch will be the same.
                # (e.g. in dump() function)
                continue
            processed.add(id(rule))
            for key_param in rule.key_params:
                key = getattr(rule, key_param)
                setattr(rule, key_param, keys[key])

    def build(self, eps, root_key, print_out=False):
        if print_out:
            print("")
            print("[[API OUTPUT STARTS]]")

        if print_out:
            print('number of rules:', len(self._rules))
        for rule in self._rules:
            rule.build(eps, print_out=print_out)

        if print_out:
            print("[[API OUTPUT ENDS]]")

        res = eps[root_key]

        assert res is not None

        return res

    def copy(self, deep=False):
        return list(self._rules) if not deep else deepcopy(self._rules)

    def add(self, other_rules):
        processed = set(id(rule) for rule in self._rules)
        for rule in other_rules:
            if id(rule) in processed:
                # this can happen when we are merging the branch of the source with this source.
                # In this case some rules in the source and the branch will be the same.
                # (e.g. in dump() function)
                continue
            processed.add(id(rule))
            self._rules.append(rule)
