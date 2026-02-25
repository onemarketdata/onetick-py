from pathlib import Path
from setuptools import setup


with open('src/onetick/py/_version.py') as f:
    version = None
    for line in f:
        if line.startswith('#'):
            continue
        _, _, version = line.partition('=')
        version = version.strip().strip("'").strip('"')
    if not version:
        raise RuntimeError("Can't find version in src/onetick/py/_version.py")

setup(name='onetick-py',
      version=version,
      entry_points={
          'console_scripts': [
              'onetick-render = onetick.py.utils.render_cli:main',
              'jupyter-onetick_snippets = onetick.doc_utilities.snippets:main',
          ],
      })
