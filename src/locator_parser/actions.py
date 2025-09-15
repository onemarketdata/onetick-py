from locator_parser.common import Action, Entity
from collections import OrderedDict


class Add(Action):
    """
    Add entity.

    Example:
    raw_db = Add(RawDB(id='PRIMARY'))
    raw_db.add_where(DB, id='DB_2')
    """

    def __init__(self, obj):
        self.what_to_add = obj.to_lines()

        super().__init__()

        if obj.__class__.PARENT is not Entity:
            self._add_conditions[(obj.__class__.PARENT.TAG.lower(), None, None)] = False

    def do(self, obj, all_lines, properties, writer):
        writer.add(self.what_to_add)


class Modify(Action):
    """
    Modify entity properties.
    If property doesn't exist, then it will be added.
    If property is set to None, then it will be removed.

    Example:
    action = Modify(symbology='TDEQ', db_archive_tmp_dir=None)
    action.add_where(DB, id="DB_1")
    """

    def __init__(self, **kwargs):
        if len(kwargs) > 1:
            raise ValueError("It is not supported to set multiple keys yet!")

        field, value = "", ""
        delete = False

        for k, v in kwargs.items():
            if v is None:
                delete = True

            field, value = k, str(v)

        self.init(field, value, delete)

        super().__init__()

    def init(self, field, value, delete):
        self.field = field.lower()
        self.value = '"' + value + '"'
        self.delete = delete

    def do(self, obj, all_lines, properties_lines, writer):
        if not obj.__class__.HAS_PROPERTIES:
            raise AttributeError("Entity with tag '" + obj.__class__.TAG + "' does not expect to have properties!")

        new_properties = list(properties_lines)

        if not hasattr(obj, self.field):
            for line in new_properties:
                end_pos = line.find(">")

                if end_pos != -1:
                    if line.find("/>") != -1 and line.find("/>") < end_pos:
                        end_pos = line.find("/>")

                    line = line[:end_pos] + " %s=%s" % (self.field, self.value) + line[end_pos:]
                    new_properties[-1] = line
                    break
        else:
            for inx, line in enumerate(new_properties):
                pos = line.lower().find(self.field + "=")

                if pos != -1:
                    # modify property
                    t_line = line[pos + len(self.field):].split()[0].split("/>")[0].split(">")[0].split("=")[1]

                    if self.delete:
                        line = line.replace(line[pos: pos + len(self.field) + 1] + t_line, "")
                    else:
                        line = line.replace(
                            line[pos: pos + len(self.field) + 1] + t_line,
                            line[pos: pos + len(self.field) + 1] + self.value,
                        )
                    new_properties[inx] = line
                    break

        writer.modify(len(all_lines), properties_lines, new_properties)


class Delete(Action):
    """
    Delete entity

    Example:
    action = Delete()
    action.add_where(DB, id="DB_1")
    """

    def do(self, obj, all_lines, properites_lines, writer):
        writer.delete(all_lines)


class Get(Action):
    """
    Populate self.result object with properites of found object

    Example:
    action = Get()
    action.add_where(DB, id='LB')
    """

    def do(self, obj, all_lines, properties_lines, writer):
        self.lines = all_lines

        # self.result = obj.__class__()
        properties_dict = {}

        properties_lines = " ".join(properties_lines)

        # variables to concat space separated
        # values
        res_token = ""
        double_quotes_count = 0
        single_quotes_count = 0

        for token in filter(lambda x: x not in ("", "<", "/>", ">"), properties_lines.split()[1:]):
            # ---------------------
            # Logic to combine values in quotes are separated
            # by space
            if '"' in token:
                double_quotes_count += token.count('"')
            if "'" in token:
                single_quotes_count += token.count("'")

            if double_quotes_count > 0 or single_quotes_count > 0:
                if (double_quotes_count + single_quotes_count) % 2 == 0:
                    res_token += token
                    double_quotes_count = 0
                    single_quotes_count = 0
                else:
                    res_token += token
                    continue
            else:
                res_token = token
            # --------------------

            t_list = res_token.split("=")

            # refresh to do not concat
            # already ready token
            res_token = ""
            double_quotes_count = 0
            single_quotes_count = 0

            key, value = t_list[0], t_list[1]

            if value.endswith("/>"):
                value = value[:-2]

            if value.endswith(">") or value.endswith("/"):
                value = value[:-1]

            properties_dict[key.lower()] = value.replace('"', "")
            # setattr(self.result, key.lower(), value.replace('"', ''))

        self.result = obj.__class__(**properties_dict)

        for key, value in properties_dict.items():
            setattr(self.result, key, value)


class GetAll(Action):

    def __init__(self):
        super().__init__()
        self.result = []

    def do(self, obj, all_lines, properties_lines, writer):
        get = Get()
        get.do(obj, all_lines, properties_lines, writer)

        self.result.append(get.result)

    def apply(self, tag, entity):
        if self.executed:
            self.just_executed = False
            self.executed = False

            # Base is one that we are looking for; this entity has only
            # tag specified.
            # Here is logic to refresh the base, because we have to go
            # through the subtree.
            is_base = False
            for key, value in reversed(self.conditions.items()):
                t, f, v = key

                if f is None and v is None:
                    is_base = True

                if is_base:
                    self.conditions[key] = False
                    # leave, because we support only the last open-ended base
                    break

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


class Show(Action):
    """
    Print object and all children objects

    Example:
    action = Show()
    action.add_where(DB, id='S_ORDERS_LB')
    """

    def do(self, obj, all_lines, properties_lines, writer):
        print("\n".join(all_lines))


class DoNothing(Action):
    """
    Default action that does nothing
    """

    def do(self, *args, **kwargs):
        pass
