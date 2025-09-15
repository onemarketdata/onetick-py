import re

from collections import OrderedDict
from inspect import Parameter
from textwrap import dedent


class Docstring:

    def __init__(self, doc: str):
        if isinstance(doc, list):
            doc = '\n'.join(doc)
        self.doc = doc
        self.indentation = ' ' * (len(doc.lstrip('\n')) - len(doc.lstrip()))
        self.docstring = self._parse()  # NOSONAR

        self.order = ('Description',
                      'Parameters',
                      'Return',
                      'Returns',
                      'Note',
                      'Notes',
                      'Example',
                      'Examples',
                      'See Also')

    def _parse(self):
        def get_prev_name(s: str):
            doc_, name_ = s.rstrip('\n').rsplit('\n', maxsplit=1)
            prefix_ = name_
            name_ = name_.strip()
            prefix_ += '\n' + ' ' * (len(prefix_) - len(name_)) + '-' * len(name_)
            return doc_, name_, prefix_

        docstring = OrderedDict()
        res = re.split(' *?-{4,}', self.doc)  # NOSONAR
        name = 'Description'
        prefix = ''

        # if there is no description part
        if len(res[0].strip().splitlines()) == 1 and len(res) > 1:
            name = res.pop(0).strip()
            prefix = self.indentation + name + '\n' + self.indentation + '-' * len(name)

        for i, part in enumerate(res):
            if i == len(res) - 1:
                docstring[name] = prefix + part
                break
            d, next_name, next_prefix = get_prev_name(part)
            docstring[name] = prefix + d
            name, prefix = next_name, next_prefix
        return docstring

    def get_sig(self, add_self=False):
        """
        Get signature from existing documentation string.
        """
        text = self.docstring.get('Parameters')
        if not text:
            return []
        text = dedent(text)
        text = re.split('-{4,}', text)[1]
        parameters = []
        for line in text.splitlines():
            if not line or line[0] in (' ', '\t'):
                continue
            name, _, _ = line.partition(':')
            if name == 'self' and not add_self:
                continue
            parameters.append(Parameter(name=name, kind=Parameter.POSITIONAL_OR_KEYWORD))
        return parameters

    def __setitem__(self, key, value):
        if key not in self.docstring:
            if key == 'Description':
                self.docstring[key] = ''
            else:
                self.docstring[key] = self.indentation + key + '\n'
                self.docstring[key] += self.indentation + '-' * len(key) + '\n'
        for line in value.split('\n'):
            self.docstring[key] += self.indentation + line + '\n'

    def _apply_order(self):
        key_map = dict((k.lower(), k) for k in self.docstring.keys())
        for k in self.order[::-1]:
            if k.lower() in key_map:
                self.docstring.move_to_end(key_map[k.lower()], last=False)

    def build(self):
        self._apply_order()
        res = ''
        for k, value in self.docstring.items():
            res += value + '\n'

        return res

    def update(self, other: 'Docstring'):
        for key, value in other.docstring.items():

            content = value.split('\n')
            if key == 'Description':
                content = content[1:]
                if not '\n'.join(content).strip():
                    continue
            else:
                content = content[2:]

            if key in self.docstring:
                self.docstring.pop(key)

            for line in content:
                self[key] = line[len(self.indentation):]
