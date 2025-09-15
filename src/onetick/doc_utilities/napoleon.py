from typing import List
from sphinx.ext.napoleon.docstring import NumpyDocstring
from sphinx.locale import get_translation
from onetick.doc_utilities.snippets import parse_string


class OTNumpyDocstring(NumpyDocstring):

    def _parse_examples_section(self, section: str) -> List[str]:

        """Applies OT directives for all lines"""

        # combining super()._parse_examples_section and super()._parse_generic_section
        # due to super()._parse_examples_section just prepare parameters and
        # super()._parse_generic_section get lines and modify them.
        # but we need to add logic on lines but before super()._parse_generic_section modification
        labels = {
            'example': get_translation('sphinx')('Example'),
            'examples': get_translation('sphinx')('Examples'),
        }
        use_admonition = self._config.napoleon_use_admonition_for_examples
        section = labels.get(section.lower(), section)

        lines = self._strip_empty(self._consume_to_next_section())
        lines = self._dedent(lines)

        # OneTick custom logic start
        doc = parse_string('\n'.join(lines), caller='doc')
        lines = doc.split('\n')
        # OneTick custom logic end

        if use_admonition:
            header = '.. admonition:: %s' % section     # noqa
            lines = self._indent(lines, 3)
        else:
            header = '.. rubric:: %s' % section     # noqa
        if lines:
            return [header, ''] + lines + ['']
        else:
            return [header, '']
