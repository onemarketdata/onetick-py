import pytest
from onetick.doc_utilities.snippets import Snippet, Snippets, parse_string
import doctest


class TestSnippet:

    def test_append(self):
        s = Snippet()
        s.append(doctest.Example('source', 'want'))
        assert s.name is None
        assert s.code == ['source']
        e = doctest.Example('source1', 'want1')

        e.name = 'name'
        s.append(e)
        assert s.name == 'name'
        assert s.code == ['source', 'source1']
        e = doctest.Example('source1', 'want1')
        e.name = 'name1'
        with pytest.raises(ValueError, match='Two names'):
            s.append(e)

    def test_skip(self):
        s = Snippet()
        e = doctest.Example('source1', 'want1')
        e.skip = True
        e.name = 'name'
        s.append(e)
        e = doctest.Example('source', 'want')
        s.append(e)
        assert s.name == 'name'
        assert s.code == ['source']

    def test_only_dierctive(self):
        s = Snippet()
        e = doctest.Example('', 'want1')
        e.name = 'eman'
        s.append(e)
        assert s.name == 'eman'
        assert not s.code


class TestSnippets:

    def test_append(self):
        snippets = Snippets()
        snippets.append(Snippet(examples=[doctest.Example(source='asd', want='')]))
        assert not snippets._snippets   # nameless snippet was not added
        snippets.append(Snippet(name='asd', examples=[doctest.Example(source='asd', want='')]))
        with pytest.raises(ValueError, match='not unique'):
            snippets.append(Snippet(name='asd', examples=[doctest.Example(source='qwe', want='')]))
        snippets.append(Snippet(name='asd1', examples=[doctest.Example(source='zxc', want='')]))

    def test_list_view(self):
        snippets = Snippets()
        snippets.append(Snippet(name='a', examples=[doctest.Example(source='a\nb', want='zxc')]))
        snippets.append(Snippet(name='b', examples=[doctest.Example(source='c', want='asd')]))
        res = snippets.dict_view()
        assert res == {'snippets': [{'name': 'a',
                                     'code': ['a', 'b']},
                                    {'name': 'b',
                                     'code': ['c']}]}

    def test_menu_view_1(self):
        snippets = Snippets()
        snippets.append(Snippet(name='a', examples=[doctest.Example(source='1\n2\n', want='asdqwe')]))
        res, _ = snippets.menu_view()
        assert res == [{'name': 'a',
                        'snippet': ['1', '2']}]

    def test_menu_view_2(self):
        snippets = Snippets()
        snippets.append(Snippet(name='a.b', examples=[doctest.Example(source='1\n2', want='asdqwe')]))
        snippets.append(Snippet(name='a.c', examples=[doctest.Example(source='3\n4', want='')]))
        res, _ = snippets.menu_view()
        assert res == [{'name': 'a',
                        'sub-menu': [
                            {'name': 'b',
                             'snippet': ['1', '2']},
                            {'name': 'c',
                             'snippet': ['3', '4']}
                        ]}]

    def test_menu_view_3(self):
        snippets = Snippets()
        snippets.append(Snippet(name='a.b.d.e', examples=[doctest.Example(source='1\n2', want='')]))
        snippets.append(Snippet(name='a.c.d.f', examples=[doctest.Example(source='3\n4\n', want='')]))
        snippets.append(Snippet(name='a.c.g.f', examples=[doctest.Example(source='5', want='')]))
        snippets.append(Snippet(name='a.c.g.e', examples=[doctest.Example(source='6', want='')]))
        snippets.append(Snippet(name='a.c.a', examples=[doctest.Example(source='7', want='')]))
        res, _ = snippets.menu_view()
        assert res == [{'name': 'a',
                        'sub-menu': [{'name': 'b',
                                      'sub-menu': [{'name': 'd',
                                                    'sub-menu': [{'name': 'e',
                                                                  'snippet': ['1', '2']}]}]},
                                     {'name': 'c',
                                      'sub-menu': [{'name': 'd',
                                                    'sub-menu': [{'name': 'f',
                                                                  'snippet': ['3', '4']}]},
                                                   {'name': 'g',
                                                    'sub-menu': [{'name': 'f', 'snippet': ['5']},
                                                                 {'name': 'e', 'snippet': ['6']}]},
                                                   {'name': 'a', 'snippet': ['7']}]}]}]
