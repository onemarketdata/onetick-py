from collections import defaultdict

from onetick.py.core._internal._nodes_history import _NodesHistory


class _ProxyNode:
    """
    This class wraps node in _Source with goal to track changes
    """

    def __init__(self, ep, key, _, out_pin, refresh_func=None):
        self._ep = ep
        self._key = key
        self._out_pin = out_pin
        self._name = ""
        self._refresh_func = refresh_func

        self._hist = _NodesHistory()
        self._hist.assign(ep, key)

    def sink(self, p_out_pin, ep, key, in_pin, _, move_node=True):
        """
        Connect self._ep[p_out_pin|self._out_pin] -> ep[in_pin]
        """
        self._refresh_func()

        if not p_out_pin:
            p_out_pin = self._out_pin

        self._hist.sink(self._ep, self._key, p_out_pin, ep, key, in_pin)

        t_p_ep = self._ep
        if p_out_pin:
            t_p_ep = t_p_ep[p_out_pin]

        t_ep = ep
        if in_pin:
            t_ep = ep

        if move_node:
            t_p_ep.sink(t_ep)
            self._ep = ep
            self._key = key
            #  it is not the first, then no need to store this pin
            self._out_pin = None
            self._name = ""

        return self._ep

    def source(self, p_in_pin, ep, key, _, out_pin):
        """
        Connect self._ep[p_in_pin] <- ep[out_pin]
        """
        self._refresh_func()

        self._hist.source(self._ep, self._key, p_in_pin, ep, key, out_pin)

        t_p_ep = self._ep
        if p_in_pin:
            t_p_ep = t_p_ep(p_in_pin)

        t_ep = ep
        if out_pin:
            t_ep = t_ep[out_pin]

        t_p_ep.source(t_ep)

        return self._ep

    def source_by_key(self, to_key, ep, key, _, out_pin):
        """
        Connect node with key=to_key <- ep[out_pin]
        """
        self._refresh_func()

        self._hist.source_by_key(to_key, ep, key, out_pin)

        return self._ep

    def node_name(self, name=None, key=None):
        if name is not None:
            if key:
                # set node_name by key
                self._hist.node_name(key, name)
            else:
                self._hist.node_name(self._key, name)
                self._name = name
            return None
        else:
            return self._name

    def tick_type(self, tt):
        self._refresh_func()
        self._hist.tick_type(self._ep, self._key, tt)
        self._ep.tick_type(tt)

    def symbol(self, symbol):
        self._refresh_func()
        self._hist.symbol(self._ep, self._key, symbol)
        self._ep.set_symbol(symbol)

    def get(self):
        return self._ep

    def key(self, _key=None):
        if _key:
            self._key = _key

        return self._key

    def out_pin(self, _out_pin=None):
        if _out_pin:
            self._out_pin = _out_pin

        return self._out_pin

    def copy_graph(self, eps=None, print_out=False):
        if eps is None:
            eps = defaultdict()

        return self._hist.build(eps, self._key, print_out=print_out), self._key, None, self._out_pin

    def rebuild_graph(self, keys: dict):
        """
        Changing all uuids in this node and it's history.
        Need it for making a deep copy of node.

        Parameters
        ----------
        keys: dict
            Mapping from old key to new key
        """
        self._key = keys[self._key]
        self._hist.rebuild(keys)

    def copy_rules(self, deep=False):
        return self._hist.copy(deep=deep)

    def add_rules(self, rules):
        self._hist.add(rules)
