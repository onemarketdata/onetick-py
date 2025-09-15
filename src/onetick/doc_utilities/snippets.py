import os
import argparse
import doctest
import pkgutil
import json
from collections import defaultdict
from typing import List, Optional
from jupyter_core.paths import jupyter_data_dir, jupyter_config_dir
import onetick.py as otp
from onetick.doc_utilities.ot_doctest import OTDoctestParser


def parse_args():
    parser = argparse.ArgumentParser(description='Adding snippets configuration from onetick-py package')
    parser.add_argument('extension',
                        choices=['snippets', 'snippets_menu', 'jupyterlab_snippets'],
                        default='snippets', nargs='?',
                        help='adding configuration to selected extension')
    parser.add_argument('-i', '--names-info', action='store_true',
                        help='show snippet names as yaml "tree"')
    args = parser.parse_args()
    return args


def collect_examples_from_obj(obj: doctest.DocTest) -> List:
    """groups examples for one object (docstring)"""
    snippets = []
    id_ = 1
    snippet = Snippet(obj_ref=f'{obj.name} {id_}')

    for example in obj.examples:
        snippet.append(example)
        if example.want:
            snippets.append(snippet)
            id_ += 1
            snippet = Snippet(obj_ref=f'{obj.name} {id_}')
    return snippets


def parse_string(string: str, caller: Optional[str] = None) -> str:
    parser = OTDoctestParser(caller=caller)
    lines = parser.parse(string)
    res: List = []
    snippet = Snippet()
    for item in lines:
        if isinstance(item, doctest.Example):
            snippet.append(item)
        else:
            if snippet.code:
                res.append(snippet)
                snippet = Snippet()
            res.append(item)
    return ''.join(map(str, res))


class SnippetInspectionError(Exception):
    pass


def inspect_modules():

    """Inspect onetick.py modules and collect snippets"""

    finder = doctest.DocTestFinder(parser=OTDoctestParser(caller='snippet'))
    snippets = Snippets()

    error_messages = []

    for _, modname, _ in pkgutil.walk_packages(otp.__path__, prefix=otp.__name__ + '.'):
        module = __import__(modname, fromlist=['__any__'])
        for doc in finder.find(module):
            for snippet in collect_examples_from_obj(doc):
                try:
                    snippets.append(snippet)
                except Exception as e:
                    error_messages.append(str(e))
    if error_messages:
        error_messages = '\t\n'.join(error_messages)
        message = f"During collection of snippets following exception were raised:\n{error_messages}"
        raise SnippetInspectionError(message)

    return snippets


def set_snippets(content):
    """Set nbextension-contrib-snippets with content"""
    snippets = {'snippets': content}
    path = jupyter_data_dir()
    path = os.path.join(path, 'nbextensions', 'snippets', 'snippets.json')
    if not os.path.exists(path):
        raise FileNotFoundError('Not able to set onetick snippets. Make sure nbextension snippets was installed.'
                                f'File {path} not found')
    with open(path, 'w') as f:
        json.dump(snippets, f, indent=4, sort_keys=True)


def set_snippets_menu(content):
    """Set nbextension-contrib-snippets_menu with content"""
    custom_js = f'''require(["nbextensions/snippets_menu/main"], function (snippets_menu) {{
    console.log('Loading `snippets_menu` customizations from `custom.js`');
    var horizontal_line = '---';
    var snippets = {{
        'name' : 'OneTick snippets',
        'sub-menu' : {json.dumps(content, indent=4, sort_keys=True)}
    }};
    snippets_menu.options['menus'].push(snippets);
    console.log('Loaded `snippets_menu` customizations from `custom.js`');
}});'''
    path = os.path.join(jupyter_config_dir(), 'custom', 'custom.js')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(custom_js)


def set_jupyterlab_snippets(content):
    """Set jupyterlab-snippets with content"""

    def traverse_snippets(path, node_list):
        for node in node_list:
            if "sub-menu" in node:
                folder_path = os.path.join(path, node["name"])
                os.makedirs(folder_path, exist_ok=True)
                traverse_snippets(folder_path, node["sub-menu"])
            elif "snippet" in node:
                filename = os.path.join(path, node["name"].replace("|", " ").replace("/", " ")) + ".py"
                print(filename)
                with open(filename, 'w') as f:
                    f.write("\n".join(node["snippet"]))

    snippet_path = os.path.join(jupyter_data_dir(), "snippets", "OneTick snippets")
    os.makedirs(snippet_path, exist_ok=True)
    traverse_snippets(snippet_path, content)


class Snippet:

    """
    Snippet container.
    Contain code from doctest.Examples, add ability to set name.
    Validate names get from doctest.Examples
    """

    def __init__(self,
                 name: Optional[str] = None,
                 examples: Optional[List[doctest.Example]] = None,
                 obj_ref: Optional[str] = None):
        self._name = name
        self._examples = examples or []
        self._obj_ref = obj_ref
        self.skip_snippet = False

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if self._name:
            raise ValueError(f'Two names set for one snippet in "{self._obj_ref}": {self._name}, {value}')
        self._name = value

    @property
    def code(self) -> List[str]:
        res = []
        for ex in self._examples:
            res.extend(ex.source.strip().split('\n'))
        return res

    @property
    def code_raw(self) -> str:
        res = []
        for ex in self._examples:
            for i, line in enumerate(ex.source.strip().split('\n')):
                if i == 0:
                    res.append('>>> ' + line)
                else:
                    res.append('... ' + line)
            res.append(ex.want)
        return '\n'.join(res)

    def append(self, item: doctest.Example):
        for attr in ['name', 'skip_snippet']:
            if hasattr(item, attr):
                setattr(self, attr, getattr(item, attr))
        skip_example = False
        if hasattr(item, 'skip'):
            skip_example = getattr(item, 'skip')
        if not skip_example and item.source.strip():
            self._examples.append(item)

    def __str__(self):
        return self.code_raw


class Snippets:

    """Container for Snippet's"""

    def __init__(self):
        self._snippets = {}

    def append(self, item: Snippet):
        """
        Validate Snippet name;
        Add Snippet to container
        """
        if item.skip_snippet or not item.code:
            return
        if not item.name:
            return  # skipping nameless snippets
        if item.name in self._snippets:
            raise ValueError('Snippet name is not unique. '
                             f'Objects "{self._snippets[item.name]._obj_ref}", "{item._obj_ref}"')
        self._snippets[item.name] = item

    def extend(self, items: List):
        for item in items:
            self.append(item)

    def __iter__(self):
        yield from self._snippets.values()

    def dict_view(self):
        """Format snippets for nbextension-contrib-snippets"""
        return {"snippets": [{"name": s.name, "code": s.code} for s in self]}

    def menu_view(self):
        """Format snippets for nbextension-contrib-snippets_menu"""

        def tree():
            return defaultdict(tree)

        def set(t, keys: List, value):
            tmp = t
            for key in keys[:-1]:
                tmp = tmp[key]
            tmp[keys[-1]] = value

        def walk(t):
            res = []
            names = []
            for key in t.keys():
                if not isinstance(t[key], defaultdict):
                    res.append({"name": key, "snippet": t[key].code})
                    names.append(key)
                else:
                    code, name = walk(t[key])
                    res.append({"name": key, "sub-menu": code})
                    names.append({key: name})
            return res, names

        res = tree()

        for v in self:
            names = v.name.split('.')
            set(res, names, v)
        return walk(res)


def main():
    args = parse_args()
    snippets = inspect_modules()
    if args.names_info:
        import yaml
        _, menu = snippets.menu_view()
        print(yaml.dump(menu))
    else:
        if args.extension == 'snippets':
            set_snippets(snippets.dict_view())
        elif args.extension == 'snippets_menu':
            s, _ = snippets.menu_view()
            set_snippets_menu(s)
        elif args.extension == 'jupyterlab_snippets':
            s, _ = snippets.menu_view()
            set_jupyterlab_snippets(s)


if __name__ == '__main__':
    main()
