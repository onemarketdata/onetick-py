from typing import Optional, Union


class RemoteTS:
    PROTOCOLS = ('ws', 'wss', 'http')

    def __init__(
        self,
        host: Union[str, 'LoadBalancing', 'FaultTolerance'],
        port: Optional[Union[int, str]] = None,
        protocol: Optional[str] = None,
        resource: Optional[str] = None,
        cep: bool = False,
    ):
        """
        Class representing remote tick-server.
        Can be :py:meth:`used <onetick.py.Session.use>`
        in :py:class:`~onetick.py.Session` as well as the local databases.

        Parameters
        ----------
        host: str, :py:class:`~onetick.py.servers.LoadBalancing`, :py:class:`~onetick.py.servers.FaultTolerance`
            In case of string: string with this format: ``[proto://]hostname:port[/resource]``.
            Parameters in square brackets are optional.

            Otherwise, configuration of LoadBalancing and/or FaultTolerance (please, check corresponding classes)
        port: int, str, optional
            The port number of the remote tick-server.
            If not specified here, can be specified in the ``host`` parameter.
        protocol: str, optional
            The protocol to connect with.
            If not specified here, can be specified in the ``host`` parameter.
        resource: str, optional
            The resource of the host.
            If not specified here, can be specified in the ``host`` parameter.
        cep: bool
            Specifies if the remote server is the CEP-mode tick-server.

        Examples
        --------

        Specify host name and port together in first parameter:

        >>> session.use(otp.RemoteTS('server.onetick.com:50015'))  # doctest: +SKIP

        Additionally specify websocket protocol and resource of the remote server:

        >>> session.use(otp.RemoteTS('wss://data.onetick.com:443/omdwebapi/websocket'))  # doctest: +SKIP

        Combination of LoadBalancing and FaultTolerance can be used for host parameter:

        >>> RemoteTS(FaultTolerance(LoadBalancing('host1:4001', 'host2:4002'),
        ...                         LoadBalancing('host3:4003', 'host3:4004')) # doctest: +SKIP
        """
        self.cep = cep

        self._protocol = protocol
        self._host = host
        self._port = port
        self._resource = resource

        if not isinstance(self._host, str):
            if self._port or self._protocol or self._resource:
                raise ValueError('When host is not a string, other parameters cannot be specified')
            return

        protocol, host, port, resource = self._parse_url(host)
        if any([protocol, port, resource]) and any([self._protocol, self._port, self._resource]):
            raise ValueError('Mixed url parts in `host` parameter and in other parameters')

        self._protocol = self._protocol or protocol
        self._host = host
        self._port = self._port or port
        self._resource = self._resource or resource

        if self._protocol and self._protocol not in self.PROTOCOLS:
            raise ValueError(f"Protocol '{self._protocol}' is not supported")

        if not self._port:
            raise ValueError('Port is not set')

    def _parse_url(self, url):
        proto = None
        if '://' in url:
            proto, _, url = url.partition('://')
        resource = None
        if '/' in url:
            url, _, resource = url.partition('/')
        port = None
        if ':' in url:
            url, _, port = url.partition(':')
        hostname = url
        return proto, hostname, port, resource

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        if isinstance(self._host, (FaultTolerance, LoadBalancing)):
            return str(self._host)
        url = f'{self._host}:{self._port}'
        if self._protocol:
            url = f'{self._protocol}://{url}'
        if self._resource:
            url = f'{url}/{self._resource}'
        return url


class LoadBalancing:
    def __init__(self, *sockets: str):
        """
        Class representing configuration of client-side load-balancing between multiple tick servers.
        Each client query selects the target tick server from the provided list according to the load-balancing policy.
        Currently only `SERVER_LOAD_WEIGHTED` policy is available -  tick servers are chosen at random using a weighted
        probability distribution, using inverted CPU usage for weights.
        LoadBalancing class is used only with :py:class:`~onetick.py.servers.RemoteTS` and
        :py:class:`~onetick.py.servers.FaultTolerance`.

        Parameters
        ----------
        sockets: str
            Sockets used in load-balancing configuration.

        Examples
        --------
        >>> LoadBalancing('host1:4001', 'host2:4002', 'host3:4003') # doctest: +SKIP
        """
        if len(sockets) < 2:
            raise ValueError(f'There must be at least 2 sockets for load balancing but {len(sockets)} was provided')
        self.sockets = sockets

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return f"({','.join(self.sockets)})"


class FaultTolerance:
    def __init__(self, *sockets: Union[str, 'LoadBalancing']):
        """
        Class representing configuration of client-side tick-server fault tolerance.
        Tick servers in the fault tolerance list are selected according to their level of priority,
        which is from left to right. Backup servers (servers after the first one, primary) are used only if the query
        on the primary server failed or server is down. So, if the first server is down the second server is used,
        if both of them are failed the third one used and so on.
        FaultTolerance class is used only with :py:class:`~onetick.py.servers.RemoteTS`.

        Parameters
        ----------
        sockets: str, LoadBalancing
            Sockets and/or LoadBalancing groups used in fault tolerance configuration.

        Examples
        --------
        >>> FaultTolerance('host1:4001', 'host2:4002', 'host3:4003') # doctest: +SKIP

        It is possible to have multiple load-balancing groups thus mixing together fault tolerance and load-balancing.
        The selection between different load balancing groups is also done according to their priority level,
        which is from left to right.

        >>> FaultTolerance(LoadBalancing('host1:4001', 'host2:4002'),
        ...                LoadBalancing('host3:4003', 'host3:4004')) # doctest: +SKIP
        """
        if len(sockets) < 2:
            raise ValueError(f'There must be at least 2 sockets for fault tolerance but {len(sockets)} was provided')
        self.sockets = sockets

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return ','.join(map(str, self.sockets))
