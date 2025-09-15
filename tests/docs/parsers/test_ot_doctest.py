import doctest
import pytest
from onetick.doc_utilities import OTDoctestParser, register_ot_directive, apply_directive
from onetick.doc_utilities.snippets import parse_string


class TestApplyDirective:

    def test_apply_name_example(self):
        doc = "1+1     # OTdirective: snippet-name: asd;"
        e = doctest.Example(doc, '')
        e = apply_directive(e)
        assert isinstance(e, doctest.Example)
        assert e.source == '1+1\n'
        assert e.name == 'asd'

    def test_apply_name_str(self):
        doc = "1+1     # OTdirective: snippet-name: asd;"
        e = apply_directive(doc)
        assert isinstance(e, str)
        assert e == '1+1'

    @pytest.mark.parametrize('is_example', [True, False])
    def test_skip_example(self, is_example):
        doc = "1+1     # OTdirective: skip-example:;"
        if is_example:
            doc = doctest.Example(doc, '')
        res = apply_directive(doc)
        if is_example:
            assert res.skip is True
        else:
            assert res is None

    def test_unknown_directive(self):
        with pytest.raises(KeyError, match='Unknown directive'):
            apply_directive('asd # OTdirective: unknown:;')

    def test_custom_directive(self):
        register_ot_directive('add', lambda x, y: x + y)

        doc = "asdqwe # OTdirective: add: zxc;"
        res = apply_directive(doc)
        assert res == 'asdqwezxc'

    def test_two_directives(self):
        register_ot_directive('add1', lambda x, y: x + y)
        register_ot_directive('add2', lambda x, y: x + 2 * y)
        assert apply_directive('aa # OTdirective: add1: 1; add2: 2;') == 'aa122'
        assert apply_directive('aa # OTdirective: add2: 1; add1: 2;') == 'aa112'
        assert apply_directive('aa # OTdirective: add1: 1; add1: 2;') == 'aa12'

    def test_multy_line_example(self):
        doc = """
        >>> 1+1
        ...     1+2     # OTdirective: snippet-name: eman;
        """
        doc = doctest.DocTestParser().parse(doc)[1]
        res = apply_directive(doc)
        assert res.name == 'eman'
        assert res.source == '1+1\n    1+2\n'

    def test_multy_line_example_1(self):
        doc = """
        >>> 1+1
        ...     1+2     # OTdirective: snippet-name: eman;
        ...     1+3
        """
        doc = doctest.DocTestParser().parse(doc)[1]
        res = apply_directive(doc)
        assert res.name == 'eman'
        assert res.source == '1+1\n    1+2\n    1+3\n'

    def test_multy_line_example_2(self):
        doc = """
        >>> 1+1
        ...     1+2     # OTdirective: skip-example: ;
        ...     1+3
        """
        doc = doctest.DocTestParser().parse(doc)[1]
        res = apply_directive(doc)
        assert res.skip is True

    @pytest.mark.parametrize('caller,skip', [('snippet', True),
                                             ('doc', False)])
    @pytest.mark.parametrize('is_doctest', [True, False])
    def test_skip_example_with_caller(self, caller, skip, is_doctest):
        doc = """
        >>> 1+1
        ...     1+2     # OTdirective: skip-example: snippet;
        ...     1+3
        """
        if is_doctest:
            doc = doctest.DocTestParser().parse(doc)[1]
        res = apply_directive(doc, caller)
        if is_doctest:
            assert res is not None
            if skip:
                assert res.skip is skip
            else:
                assert not hasattr(res, 'skip')
        else:
            if skip:
                assert res is None
            else:
                assert res


class TestParser:

    def test_simple(self):
        doc = """
        >>> 1+1     # OTdirective: snippet-name: asd; skip-example:;
        >>> 1+2     # OTdirective: snippet-name: asdqwe;
        """
        parser = OTDoctestParser()
        res = parser.get_examples(string=doc)
        assert len(res) == 2
        assert all(isinstance(x, doctest.Example) for x in res)
        assert res[0].name == 'asd'
        assert res[0].skip is True
        assert res[1].name == 'asdqwe'

    def test_only_directive(self):
        doc = """
        >>> # OTdirective: snippet-name: eman;
        >>> 1+1
        """
        parser = OTDoctestParser()
        res = parser.get_examples(string=doc)
        assert len(res) == 2
        assert res[0].name == 'eman'


class TestParseString:

    @staticmethod
    def lstrip_multiline(doc):
        return '\n'.join([x.lstrip() for x in doc.split('\n')])

    @staticmethod
    def drop_line(doc, numbers):
        lines = doc.split('\n')
        if isinstance(numbers, int):
            lines.pop(numbers)
        elif isinstance(numbers, list):
            numbers.sort(reverse=True)
            for i in numbers:
                lines.pop(i)
        return '\n'.join(lines)

    def test_no_directive(self):
        doc = """
        some string
        test1
        >>> 1+1
        >>> 1+2
        >>> 1+3
        ...     1+4
        1
        2
        3

        zxcasdq
        """
        res = parse_string(doc)
        assert res == self.lstrip_multiline(doc)

    def test_skip_simple(self):
        doc = """
        some string
        test1
        >>> 1+1 # OTdirective: skip-example:;
        >>> 1+2
        >>> 1+3
        ...     1+4
        1
        2
        3

        zxcasdq
        """
        res = parse_string(doc)
        expected = self.lstrip_multiline(self.drop_line(doc, 3))
        assert res == expected

    def test_skip_block(self):
        doc = """
                some string
                test1
                >>> 1+1 # OTdirective: skip-example:;
                ...     1+11
                >>> 1+2
                >>> 1+3
                ...     1+4
                1
                2
                3

                zxcasdq
                """
        res = parse_string(doc)
        expected = self.lstrip_multiline(self.drop_line(doc, [3, 4]))
        assert res == expected

    def test_skip_with_want(self):
        doc = """
                some string
                test1
                >>> 1+1
                ...     1+11
                >>> 1+2
                >>> 1+3 # OTdirective: skip-example:;
                1
                2
                3

                zxcasdq
                """
        res = parse_string(doc)
        expected = self.lstrip_multiline(self.drop_line(doc, [6, 7, 8, 9]))
        assert res == expected

    def test_block_with_want(self):
        doc = """
                some string
                test1
                >>> 1+1
                ...     1+11    # OTdirective: skip-example:;
                11
                12
                13
                >>> 1+2
                >>> 1+3
                1
                2
                3

                zxcasdq
                """
        res = parse_string(doc)
        expected = self.lstrip_multiline(self.drop_line(doc, [3, 4, 5, 6, 7]))
        assert res == expected

    def test_many_directives(self):
        doc = """
                some string
                test1
                >>> 1+1
                ...     1+11    # OTdirective: skip-example:;
                11
                12
                13
                >>> 1+2
                >>> 1+3 # OTdirective: snippet-name:asdqwe; skip-example:;
                1
                2
                3

                zxcasdq
                """
        res = parse_string(doc)
        expected = self.lstrip_multiline(self.drop_line(doc, [3, 4, 5, 6, 7, 9, 10, 11, 12]))
        assert res == expected
