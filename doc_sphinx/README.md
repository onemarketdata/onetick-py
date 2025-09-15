onetick.py documentation
========================

Documentation strategy
----------------------

The onetick.py documentation uses the autodoc sphinx extention with manual prepared structure.
It allows to expose high quality documentation for the package.
Drawbacks: every new public feature should be manually added into the structure.

The onetick.query documentation (api/otq\_api) uses the autosummary sphinx extention.
It generates docs bases on the existed code there.
We use custom templates (the \_template subfolder) to hide many utilitary methods and properties
on EPs inherited from the base class. Otherwise every class would have a lot of usless methods
and generation time would be significaly increased.

Adding new API entities
-----------------------

Do not forget to include a new added entities from the ./api/ subfolder
into the \_toc.yml (table of content file)

Jupyter notebooks and their resources
-------------------------------------

Jupyter notebooks are valid type of a document that could be added into the TOC.
Example is the `./static/getting_started.ipynb` notebook.

Sphinx runs all notebooks before add them into documentation. It is quite convenient
feature to check that all notebooks work properly, however it adds into the building
time. It could be switched off in conf.py file, the `jupyter_execute_notebooks` option
set to `off`. But please do not commit this change, since we want to check integrations
in gitlab pipelines.

Static docs resources
---------------------

The static pages use doctest extention to express runnable examples. We use the pytest to validate
them, and therefore session and common resource are in the conftest.py

Generation html documentation
-----------------------------

To build the html documentation use:

    ./build.sh

Possible way to host it using python:

    cd  _build/html
    python3 -m http.server -b 0.0.0.0 <port-to-expose>

Generation markdown documentation
---------------------------------

To build the markdown documentation you need to install a plugin:

    pip install sphinx-markdown-builder

Then you use the:

    ./build.sh markdown

Generate 'internal' documentation
---------------------------------

Use the `TAGS=Internal` flag to build internal documentation

    TAGS=Internal ./build.sh

It enables pages `exclude_patterns` files from `conf.py` and blocks with scope `.. only:: Internal`

Cheat sheet
-----------

Link formatting:

- to add custom link add anchor `.. _anchor_name:` (leading underscore required) and use it with `:ref:link title <anchor_name>`
- it is also possible to add custom link to python object ```:py:class:`custom name <onetick.py.Source>` ```
- to add link with full path but show only last object use `~` (`:py:class:\`~onetick.py.Source\` &rarr; Source)
- it is also possible to link on already visible for instance in class method `Source.first` `Source`
can be linked as ```:class:`Source` ```(order of resolving link can be reversed with `.`
```:class:`.Source` ```; see also [sphinx docs](https://www.sphinx-doc.org/en/master/usage/restructuredtext/domains.html#cross-referencing-python-objects))
