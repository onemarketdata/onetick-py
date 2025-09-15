import os
import re

from locator_parser.common import Entity
from locator_parser.io import FileReader, PrintWriter
from locator_parser.actions import DoNothing


class Include(Entity):
    TAG = "INCLUDE"
    SINGLE = True
    RECURSIVELY = False

    def parse(self, reader, writer, action):
        path = self.path
        matched = re.search(r"\$\{(.*)\}", path)

        # env variable found in the path
        if matched:
            if matched.group(0)[2:-1] not in os.environ:
                raise RuntimeError("Includes use not defined environment variable: % s" % matched.group(0))

            path = path.replace(matched.group(0), os.environ[matched.group(0)[2:-1]])

        # read locator recursivly
        if Include.RECURSIVELY:
            parse_locator(FileReader(path), PrintWriter(), action, recursively=True)


class Includes(Entity):
    TAG = "INCLUDES"
    CHILDREN = [Include]


class Location(Entity):
    TAG = "LOCATION"
    SINGLE = True


class Locations(Entity):
    TAG = "LOCATIONS"
    CHILDREN = [Location]


class RawDB(Entity):
    TAG = "RAW_DB"
    CHILDREN = [Locations]
    HAS_PROPERTIES = True


class RawData(Entity):
    TAG = "RAW_DATA"
    CHILDREN = [RawDB]


class FeedOptions(Entity):
    TAG = "OPTIONS"
    SINGLE = True


class Feed(Entity):
    TAG = "FEED"
    HAS_PROPERTIES = True
    CHILDREN = [FeedOptions]


class DB(Entity):
    TAG = "DB"
    CHILDREN = [Locations, RawData, Feed]
    HAS_PROPERTIES = True


class DBs(Entity):
    TAG = "DATABASES"
    CHILDREN = [DB]


class ServerLocation(Entity):
    TAG = "LOCATION"
    SINGLE = True


class TickServers(Entity):
    TAG = "TICK_SERVERS"
    CHILDREN = [ServerLocation]


class CEPServerLocation(Entity):
    TAG = "LOCATION"
    SINGLE = True


class CEPTickServers(Entity):
    TAG = "CEP_TICK_SERVERS"
    CHILDREN = [CEPServerLocation]


class Range(Entity):
    TAG = "RANGE"
    SINGLE = True


class TimeInterval(Entity):
    TAG = "TIME_INTERVAL"
    HAS_PROPERTIES = True
    CHILDREN = [Range]


class VirtualDB(Entity):
    TAG = "DB"
    HAS_PROPERTIES = True
    CHILDREN = [TimeInterval]


class VirtualDBs(Entity):
    TAG = "VIRTUAL_DATABASES"
    CHILDREN = [VirtualDB]


def parse_locator(reader, writer, action=DoNothing(), recursively=False):
    Include.RECURSIVELY = recursively
    writer.refresh()
    reader.set_writer(writer)

    databases_p = DBs()
    v_databases_p = VirtualDBs()
    tick_servers_p = TickServers()
    cep_tick_servers_p = CEPTickServers()
    includes_p = Includes()

    for _ in reader:
        databases_p(reader, writer, action)
        v_databases_p(reader, writer, action)
        tick_servers_p(reader, writer, action)
        cep_tick_servers_p(reader, writer, action)
        includes_p(reader, writer, action)


# ----------------------------------------- #
# set parent
for _, cls in list(globals().items()):
    try:
        if issubclass(cls, Entity) and cls != Entity:
            for child in cls.CHILDREN:
                child.PARENT = Entity

                if not cls.HAS_PROPERTIES and not cls.SINGLE:
                    child.PARENT = cls
    except Exception:
        continue
