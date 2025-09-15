# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
import re
from datetime import datetime

import enchant.tokenize
from sphinx.application import Sphinx
from sphinx.ext.autodoc import (ClassDocumenter,
                                MethodDocumenter,
                                AttributeDocumenter,
                                PropertyDocumenter)


sys.path.insert(0, os.path.abspath('..'))

import onetick  #  for doc collection # noqa
import onetick.py as otp  # for version     # noqa
from onetick.py.core._source.symbol import SymbolType   # noqa
from onetick.doc_utilities.napoleon import OTNumpyDocstring  # noqa
from onetick.py.docs.docstring_parser import Docstring  # noqa


# -- Project information -----------------------------------------------------

project = 'onetick.py'
copyright = f'{datetime.now().year}, OneTick'
author = 'OneTick'

# The full version, including alpha/beta/rc tags
version = otp.__version__
rst_epilog = f'.. |version| replace:: {version}'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.


extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.doctest',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.graphviz',
    'sphinx.ext.extlinks',
    # add summary to accessors and aggregations
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx_togglebutton',
    'sphinx_copybutton',
    'myst_nb',
    'jupyter_book',
    'sphinx_thebe',
    'sphinx_comments',
    'sphinx_external_toc',
    'sphinx.ext.intersphinx',
    'sphinx_book_theme',
    'sphinxcontrib.bibtex',
    'sphinxcontrib.spelling',
    'sphinx_reredirects'
]


autosectionlabel_prefix_document = True
autosectionlabel_maxdepth = 2

# redirects
redirects = {
    'index': 'static/overview.html'
}

extlinks = {
    'pandas_main': ('https://pandas.pydata.org/docs/index.html', None),
    'pandas': ('https://pandas.pydata.org/docs/reference/api/%s.html', '%s'),
    'ray': ('https://docs.ray.io/en/latest/index.html', None),
    'graphviz': ('https://graphviz.org/%s', None),
    'python_tmpdir': ('https://docs.python.org/3/library/tempfile.html#tempfile.gettempdir', None)
}

# links to python standard library docs
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

autosummary_generate = True  # Turn on sphinx.ext.autosummary
autosummary_imported_members = True

# for some reason False value for 'members' can't be overriden, don't use it here
autodoc_default_options = {
    'show-inheritance': False,
    'member-order': 'bysource',
    'undoc-members': False,
    'private-members': False,
}
autoclass_content = 'both'
comments_config = {'hypothesis': False, 'utterances': False}

add_module_names = False
autodoc_preserve_defaults = True
autodoc_inherit_docstrings = True
autodoc_typehints = "description"

# from sphinx_copybutton, do not copy prompt characters and console output
copybutton_exclude = '.linenos, .gp, .go'

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# spelling
spelling_warning = True
spelling_word_list_filename = ['ignore_spelling.txt']
spelling_show_suggestions = True
spelling_exclude_patterns = ['static/changelog.rst', 'api/misc/oneticklib.rst']

# remove warnings
nb_mime_priority_overrides = [
    ('markdown', 'text/plain', 100),
    ('spelling', 'text/plain', 100),
]

# is markdown?
is_markdown = os.environ.get('MARKDOWN', False)


class CustomFilter(enchant.tokenize.Filter):
    def _skip(self, word):
        return (
            # spell checker doesn't ignore :py:class: and other code contructions
            word.startswith('onetick-py') or
            word.startswith('onetick.py') or
            word.startswith('otp.') or
            word.startswith('onetick.query') or
            word.startswith('otq.') or
            word.startswith('pandas') or
            word.startswith('pd.') or
            # if word is enclosed in quotes, then it's probably a name
            word[0] == '"' and word[-1] == '"' or
            word[0] == "'" and word[-1] == "'" or
            # popular file extensions
            word.endswith('.otq') or
            word.endswith('.exe') or
            word.endswith('.py') or
            word.endswith('.txt')
        )


spelling_filters = [CustomFilter]


# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['**.ipynb_checkpoints',
                    '.jupyter_cache',
                    '.pytest_cache',
                    '_build',
                    'Thumbs.db',
                    '.DS_Store',
                    'README.md']


# Example of how to enable files only if it's for internal usage
# if not tags.has('Internal'):  # noqa: E0602
#    exclude_patterns += ['static/getting_started/use_cases/bestex/*.rst']


suppress_warnings = ['toc.excluded', 'etoc.ref']

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#

html_baseurl = ''
html_favicon = ''
html_logo = 'static/logo.png'
html_show_sourcelink = False
html_show_sphinx = False
html_sourcelink_suffix = ''
html_theme = 'pydata_sphinx_theme'
html_theme_options = {
    "search_bar_text": "Type your question or search query...",
    'use_edit_page_button': False,
    "switcher": {
        # doesn't work on GitLab Pages,
        # because artifacts published in to /py-onetick/onetick-py and not in the root /
        "json_url": "/_static/switcher.json",
        "version_match": version,
    },
    "check_switcher": False,
    "navbar_end": ["version-switcher"],
    "logo": {
        "link": "static/overview",
    },
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['js/']
html_js_files = [
    "rag.js",
    "https://cdnjs.cloudflare.com/ajax/libs/marked/13.0.0/marked.min.js"
]
bibtex_bibfiles = ['references.bib']

# ------- Jupyter
nb_execution_cache_path = './.jupyter_cache'
if is_markdown:
    nb_execution_mode = 'off'
    nb_remove_code_outputs = True
else:
    nb_execution_mode = 'cache'
nb_execution_raise_on_error = True
nb_execution_timeout = 60
use_jupyterbook_latex = True

language = None

latex_engine = 'pdflatex'

myst_enable_extensions = ['colon_fence', 'dollarmath', 'linkify', 'substitution', 'tasklist']
myst_url_schemes = ['mailto', 'http', 'https']
nb_output_stderr = 'show'
numfig = True
panels_add_bootstrap_css = False
pygments_style = 'sphinx'
suppress_warnings += ['myst.domains']
use_multitoc_numbering = True
external_toc_exclude_missing = False
external_toc_path = '_toc.yml'

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = False
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = True
napoleon_use_param = True
napoleon_use_rtype = True


class OTClassDocumenter(ClassDocumenter):
    """ class for processing accessors, e.g. .str .dt .float """

    _bases_pattern = re.compile('`(.+?)`')

    def is_accessor(self):
        """check if documenting Accessor object"""
        name = self.object.__name__
        if 'Accessor' in name or name == '_Operation':
            return True
        return False

    def format_name(self) -> str:
        """
        if accessor - will reformat classname

        convert _AccessorFloat to float
        """
        result = super().format_name()
        if self.is_accessor():
            result = result.strip('_').replace('Accessor', '').lower()
        return result

    def format_signature(self, **kwargs):
        """
        Drops signature for Accessor
        Don't need to show signature for accessors, just accessor name
        """
        if self.is_accessor():
            return ''
        return super(OTClassDocumenter, self).format_signature(**kwargs)

    def add_line(self, line: str, source: str, *lineno: int) -> None:
        """
        Drops Bases to protected classes
        """
        if 'Bases' in line:
            res = re.findall(self._bases_pattern, line)
            if res and any(entry.split('.')[-1].startswith('_') for entry in res):
                return
        super().add_line(line, source, *lineno)


class AccessorMethodDocumenter(MethodDocumenter):
    def format_name(self) -> str:
        # omit accessor name which is added by Sphinx after class's format_name
        result = super().format_name()
        if "Accessor" in result or '_Operation' in result:
            _, _, result = result.partition(".")
        return result


class AccessorPropertyDocumenter(PropertyDocumenter):

    def format_name(self) -> str:
        # omit accessor name which is added by Sphinx after class's format_name
        result = super().format_name()
        if "Accessor" in result or '_Operation' in result:
            _, _, result = result.partition(".")
        return result


class OTAtribDocumenter(AttributeDocumenter):

    def get_sourcename(self) -> str:
        if isinstance(self.object, SymbolType):
            return f"{self.object.__module__}.{self.object.__qualname__}"
        else:
            return super().get_sourcename()


def _process_docstring(app, what: str, name: str, obj,
                       options, lines) -> None:
    result_lines = lines
    result_lines = Docstring(result_lines).build()
    docstring = OTNumpyDocstring(result_lines, app.config, app, what, name, obj, options)
    result_lines = docstring.lines()
    lines[:] = result_lines[:]


def _skip_doc(app, what, name, obj, skip, options):
    if name == 'base_ep':
        return True
    if ('Accessor' in name and name != '_Accessor') or (name == '_Operation'):
        return False


def setup(app: Sphinx):
    app.add_autodocumenter(OTClassDocumenter, override=True)
    app.add_autodocumenter(AccessorMethodDocumenter, override=True)
    app.add_autodocumenter(AccessorPropertyDocumenter, override=True)
    app.add_autodocumenter(OTAtribDocumenter, override=True)
    app.connect('autodoc-process-docstring', _process_docstring, priority=1)
    app.connect('autodoc-skip-member', _skip_doc)
