import re
from collections import OrderedDict
from abc import ABC, abstractmethod
from typing import List, Type


def get_properties(declaration):
    """
    Construct dictionary from properties in format
    <prop_1=value_1 prop_2=value_2 ... >
    or
    <prop_1=value_1 prop_2=value_2 ... />
    and return it back
    """
    tokens = declaration.split()

    properties = {
        token.split("=")[0].lower(): token.split("=")[1].replace("/>", "").replace(">", "")
        for token in tokens[1:]
        if "=" in token
    }

    return properties


class Generator:

    def __iter__(self):
        return self

    def __next__(self):
        self.current = next(self.generator)
        return self.current


class Action(ABC):

    def __init__(self):
        self.conditions = OrderedDict()
        # additional conditions that we can deduce
        # it's applicable for the sections without properties
        #
        # For example, if you add DB, then you can forget that
        # it's situated in the single <databases> section; that
        # information we retrieve from the entities inheritance
        # put here and use along with conditions dict
        self._add_conditions = OrderedDict()

        self.executed = False
        self.just_executed = False

    @abstractmethod
    def do(self):
        pass

    def clone(self, *args, **kwargs):
        obj = self.__class__(*args, **kwargs)
        obj.conditions = dict(self.conditions)

        for key, _ in obj.conditions.items():
            obj.conditions[key] = False

        return obj

    def apply(self, tag, entity):
        if self.executed:
            self.just_executed = False
            return

        # first time it updates and then does nothing
        self.conditions.update(self._add_conditions)
        self._add_conditions = OrderedDict()

        tag = tag.lower()

        for key, value in self.conditions.items():
            t, f, v = key

            if tag == t:
                if f:
                    if hasattr(entity, f) and getattr(entity, f) == '"' + v + '"':
                        self.conditions[(t, f, v)] = True
                    else:
                        # if we move to the same similar section, but with another key,
                        # then set it to False, because in the previous section it might be
                        # set to True
                        self.conditions[(t, f, v)] = False
                else:
                    self.conditions[(t, f, v)] = True
            else:
                if value:
                    continue

            if not self.conditions[(t, f, v)]:
                break

        self.executed = self.is_result()
        self.just_executed = self.executed

    def is_result(self):
        for v in self.conditions.values():
            if not v:
                return False

        return True

    def add_where(self, tag, **kwargs):
        if not isinstance(tag, str):
            # for case when it's class name
            tag = tag.TAG

        if len(kwargs) > 1:
            raise ValueError("It is not support to set multiple keys yet!")

        if len(kwargs) == 0:
            self.conditions[(tag.lower(), None, None)] = False
        else:
            self.conditions[(tag.lower(), list(kwargs.keys())[0].lower(), list(kwargs.values())[0])] = False


def apply_actions(func, reader, writer, actions=None, flush=False):
    from locator_parser.io import LinesReader
    from locator_parser.actions import DoNothing

    if actions is None:
        actions = []

    if len(actions) == 0:
        actions.append(DoNothing())

    func(reader, writer, actions[0])

    for action in actions[1:]:
        lines = "\n".join(map(lambda x: x.replace("\n", ""), writer.lines))
        new_reader = LinesReader(lines)

        func(new_reader, writer, action)

    if flush:
        writer.flush()

    for action in actions:
        if not action.executed:
            return False

    return True


class Writer(ABC):

    NOTHING_MSG = 0
    NEW_MSG = 1
    MODIFY_MSG = 2
    DELETE_MSG = 3

    def __init__(self):
        self.refresh()

    def refresh(self):
        self.__gen = self.__generator__()
        # init generator to send further messages
        next(self.__gen)

        self.lines = []

    def __generator__(self):
        while True:
            msg_type = yield

            if msg_type == Writer.NOTHING_MSG:
                # since second value is a just next line
                line = yield
                self.lines.append(line)
            elif msg_type == Writer.NEW_MSG:
                lines = yield

                assert isinstance(lines, list)

                self.lines = self.lines[:-1] + lines + self.lines[-1:]
            elif msg_type == Writer.MODIFY_MSG:
                pos, orig_lines, new_lines = yield

                for inx, pair in enumerate(zip(orig_lines, new_lines)):
                    orig_line, new_line = pair

                    self.lines[-pos + inx] = self.lines[-pos + inx].replace(orig_line, new_line)
            elif msg_type == Writer.DELETE_MSG:
                lines_to_delete = yield

                for _ in range(len(lines_to_delete)):
                    self.lines.pop()

    def __next__(self):
        return self.__send(None)

    def __send(self, arg):
        """private method to not allow user to send something directly"""
        self.__gen.send(arg)

    def put_as_is(self, line):
        self.__send(Writer.NOTHING_MSG)
        self.__send(line)

    def add(self, lines):
        self.__send(Writer.NEW_MSG)
        self.__send(lines)

    def modify(self, pos, orig_lines, new_lines):
        self.__send(Writer.MODIFY_MSG)
        self.__send((pos, orig_lines, new_lines))

    def delete(self, lines):
        self.__send(Writer.DELETE_MSG)
        self.__send(lines)

    @abstractmethod
    def flush(self):
        pass


class Reader(Generator):

    def __init__(self):
        self.generator = self.__generator__()
        self.writer = None
        self.current = None

    def set_writer(self, writer):
        self.__writer = writer

    def __generator__(self):
        for line in self.iterable_object:
            # put just coming line as is into the writer
            self.__writer.put_as_is(line)

            line = line.strip()

            if line == "" or line[0:1] == "#":
                continue

            yield line


class Entity(Generator, ABC):

    # Tag to identify entity
    TAG = ""
    # Whether entity has properties
    HAS_PROPERTIES = False
    # Whether it is single line properties,
    # it means that entity ends with '/>' instead of '</TAG>'
    SINGLE = False
    # children entities
    CHILDREN: List[Type["Entity"]] = []

    def __init__(self, **kwargs):
        if self.__class__.SINGLE:
            self.__class__.HAS_PROPERTIES = True

        self.__tag = self.__class__.TAG.lower()

        # define condition to identify section
        pattern = "<\\s*" + self.__tag
        if self.__class__.HAS_PROPERTIES:
            pattern += "\\s+"
        else:
            pattern += "\\s*>"

        self.__cond = lambda x: re.match(pattern, x.lower())
        self.__lines = []
        self.__properties_lines = []
        self.__do = False
        self.__custom_properties = kwargs

    def to_lines(self):
        result = []
        line = "<" + self.__class__.TAG + " "

        for key, value in self.__custom_properties.items():
            line += key + '="' + str(value) + '" '

        if self.__class__.SINGLE:
            line += "/>"
            result.append(line)
        else:
            line += ">"
            result.append(line)

            offset = "    "
            for child in self.__class__.CHILDREN:
                if not child.SINGLE and not child.HAS_PROPERTIES:
                    result.append(offset + "<" + child.TAG + ">")
                    result.append(offset + "</" + child.TAG + ">")

            result.append("</" + self.__class__.TAG + ">")

        return result

    def __process(self, reader, writer, action):
        line = reader.current
        pattern = "</\\s*" + self.__tag + "\\s*>"

        while True:
            self.__lines.append(line)

            if self.__class__.SINGLE:
                if line.find("/>") != -1:
                    # the final line for single and we have to return it
                    yield line

                    if action.just_executed:
                        self.finish(writer, action)

                    return
            else:
                if re.match(pattern, line.lower()):
                    self.finish(writer, action)
                    return

            try:
                line = next(reader)
                yield line
            except StopIteration:
                return

    def finish(self, writer, action):
        if self.__do:
            action.do(self, self.__lines, self.__properties_lines, writer)

    def __read_properties__(self):
        line = self.current
        self.__properties_lines.append(line)
        declare = line

        while line.find(">") == -1:
            line = next(self)
            self.__properties_lines.append(line)
            declare += " " + line

        properties = get_properties(declare)
        self.__dict__.update(properties)

    def __process_children__(self, reader, writer, action):
        for _ in self:
            for child_p in self.__class__.CHILDREN:
                child_p()(self, writer, action)

    def __call__(self, reader, writer, action):
        if self.__cond(reader.current):
            self.generator = self.__process(reader, writer, action)
            self.current = reader.current

            self.__read_properties__()
            action.apply(self.__class__.TAG, self)

            if action.just_executed:
                self.__do = True

            self.parse(self, writer, action)

            self.__process_children__(self, writer, action)

            return True

        return False

    def parse(self, reader, writer, action):
        pass
