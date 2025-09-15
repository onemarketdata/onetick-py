from locator_parser.common import Entity
from locator_parser.actions import DoNothing


class Allow(Entity):
    TAG = "ALLOW"
    SINGLE = True


class DB(Entity):
    TAG = "DB"
    HAS_PROPERTIES = True
    CHILDREN = [Allow]


class DBs(Entity):
    TAG = "DATABASES"
    CHILDREN = [DB]


class User(Entity):
    TAG = "USER"
    SINGLE = True


class Role(Entity):
    TAG = "ROLE"
    HAS_PROPERTIES = True
    CHILDREN = [User]


class Roles(Entity):
    TAG = "ROLES"
    CHILDREN = [Role]


class EP(Entity):
    TAG = "EP"
    HAS_PROPERTIES = True
    CHILDREN = [Allow]


class EPs(Entity):
    TAG = "EVENT_PROCESSORS"
    CHILDREN = [EP]


def parse_acl(reader, writer, action=DoNothing()):
    writer.refresh()
    reader.set_writer(writer)

    roles_p = Roles()
    databases_p = DBs()
    eps_p = EPs()

    for _ in reader:
        roles_p(reader, writer, action)
        databases_p(reader, writer, action)
        eps_p(reader, writer, action)


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
