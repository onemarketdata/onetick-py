import random
import string
from contextlib import contextmanager
from functools import partial

import pytest
import pandas as pd
from onetick.test.utils import random_string, random_regex

import onetick.py as otp
from onetick.py.compatibility import is_repeat_with_field_name_works_correctly


def generate_bigger_strings(lines, only_one=True):
    """
    Generate random string (or bunch of string) which is longer than the longest one from lines.
    Takes array of strings and generate one string (in only_one param is True) or list of strings the same size
        as lines param, where each string is longer than related one from lines
        (longer than longest one from lines in case of only_one = True)

    Parameters
    ----------
    lines: List[str]
        List of strings
    only_one: bool, optional, default = True
        If True the method will return only one string, if False list of strings the same size
        as lines param

    Returns
    -------
        List of strings or one string
    """
    if only_one:
        max_len = max(len(line) for line in lines)
        return random_string(min_len=max_len + 1, max_len=max_len + 10)
    else:
        return [random_string(min_len=len(line) + 1, max_len=len(line) + 10) for line in lines]


def generate_different_alphabet_strings(lines=None, only_one=True, new_alphabet=string.digits):
    """ Generate a random string(s) from alphabet passed in new_alphabet
    Generate one string or list of string the same size as lines (if only_one is True) from non default alphabet.

    Parameters
    ----------
    lines: List[str], optional
        List of strings, isn't necessary for only_one=True case
    only_one: bool, optional, default = True
        If True the method will return only one string, if False list of strings the same size
        as lines param
    new_alphabet: str, optional
        Alphabet to generate string from.

    Returns
    -------
        List of strings or one string
    """
    if only_one:
        return random_string(min_len=1, alphabet=new_alphabet)
    else:
        return [random_string(min_len=1, alphabet=new_alphabet) for _ in lines]


class TestCommon:
    def test_access(self):
        """ only string type columns have str accessor """
        data = otp.Ticks(dict(x=["a", "b"]))
        assert data.x.dtype is str
        data.x.str

        data = otp.Ticks(dict(x=["a" * 100]))
        assert data.x.dtype is otp.string[100]
        data.x.str

        data = otp.Ticks(dict(x=[1, 2]))
        assert data.x.dtype is int
        with pytest.raises(TypeError, match="str accessor is available only for string type columns"):
            data.x.str

        data = otp.Ticks(dict(x=[0.5, 2.1]))
        assert data.x.dtype is float
        with pytest.raises(TypeError, match="str accessor is available only for string type columns"):
            data.x.str


class TestToDatetime:
    @pytest.mark.parametrize('tz', [None, 'GMT', 'EST5EDT', 'Europe/Moscow'])
    def test_default(self, tz, m_session):
        """ check default conversion """
        params = {}

        if tz:
            params['timezone'] = tz

        data = otp.Ticks(dict(x=["2005/12/01 01:45:34.123456789"]))

        data.some_date = data["x"].str.to_datetime(**params)

        df = otp.run(data, **params)

        s = pd.to_datetime(df["x"], format="%Y/%m/%d %H:%M:%S.%f")

        assert df["some_date"][0].value == s[0].value

    def test_simple(self, m_session):
        """ check to pass format """
        data = otp.Ticks(dict(x=["2005/12/01 01:45:34.123456789"]))

        data.some_date = data["x"].str.to_datetime("%Y/%m/%d %H:%M:%S.%f")

        df = otp.run(data)

        s = pd.to_datetime(df["x"], format="%Y/%m/%d %H:%M:%S.%f")

        assert df["some_date"][0].value == s[0].value

    def test_trunc(self, m_session):
        """ check to pass format """
        data = otp.Ticks(dict(x=["2005/12/01 01:45:34.123456789"]))

        data.without_msec = data["x"].str.to_datetime("%Y/%m/%d %H:%M:%S")
        data.with_msec = data["x"].str.to_datetime("%Y/%m/%d %H:%M:%S.%f")

        df = otp.run(data, timezone="GMT")

        assert df["with_msec"][0].value - df["without_msec"][0].value == 123456789

    def test_separator(self, m_session):
        """ check non default separator """
        data = otp.Ticks(dict(x=["2005|12|01 01:45:34.123"]))

        data.some_date = data["x"].str.to_datetime("%Y|%m|%d %H:%M:%S.%f")

        df = otp.run(data)

        s = pd.to_datetime(df["x"], format="%Y|%m|%d %H:%M:%S.%f")

        assert df["some_date"][0].value == s[0].value

    def test_swap_position(self, m_session):
        data = otp.Ticks(dict(x=["01|12|2005 01:45:34.123"]))

        data.some_date = data["x"].str.to_datetime("%d|%m|%Y %H:%M:%S.%f")

        df = otp.run(data)

        s = pd.to_datetime(df["x"], format="%d|%m|%Y %H:%M:%S.%f")

        assert df["some_date"][0].value == s[0].value

    def test_onetick_format_msecs(self, m_session):
        """ %J is synonym for %s """
        data = otp.Ticks(dict(x=["2005/12/01 01:45:34.123456789"]))

        data.some_date = data["x"].str.to_datetime("%Y/%m/%d %H:%M:%S.%J")

        df = otp.run(data)

        s = pd.to_datetime(df["x"], format="%Y/%m/%d %H:%M:%S.%f")

        assert df["some_date"][0].value == s[0].value

    def test_timezone(self, m_session):
        """ check default conversion """
        data = otp.Ticks(dict(x=["2005/12/01 01:45:34.123456789"]))

        data.date1 = data["x"].str.to_datetime(timezone="EST5EDT")
        data.date2 = data["x"].str.to_datetime(timezone="GMT")

        df = otp.run(data)

        assert df.date1[0] - df.date2[0] == otp.Hour(5)


class TestToken:
    def test_default(self, m_session):
        """ check default conversion """
        string = "Oh, hello, sailor!"
        data = otp.Tick(x=string)
        data.y = data["x"].str.token(sep=",", n=1)
        df = otp.run(data)
        assert df["y"][0] == string.split(sep=",")[1]

    def test_one_token(self, m_session):
        """ check the case where there is no token at all """
        string = "token"
        data = otp.Tick(x=string)
        data.y = data["x"].str.token()
        assert data['y'].dtype is str
        df = otp.run(data)
        assert df["y"][0] == string

        data.y = data["x"].str.token(n=-1)
        df = otp.run(data)
        assert df["y"][0] == string

    def test_negative_index(self, m_session):
        """ check indexes from the end """
        string = "aXbXcXd"
        data = otp.Tick(x=string)
        data.y = data["x"].str.token(sep="X", n=-2)
        df = otp.run(data)
        assert df["y"][0] == string.split(sep="X")[-2]

    def test_non_existing_index(self, m_session):
        """ out of index generates empty string """
        string = "Oh, hello, sailor!"
        data = otp.Tick(x=string)
        data.y = data["x"].str.token(sep=",", n=100)
        df = otp.run(data)
        assert df["y"][0] == ""

    def test_on_random_string(self, m_session):
        """ out of index generates empty string """
        tokens_number = random.randint(1, 10)
        tokens = [random_string() for _ in range(tokens_number)]
        sep = random.choice((" ", "-", "/", ".", ",", ":"))
        data = otp.Tick(x=sep.join(tokens))
        index = random.randint(0, tokens_number - 1)
        data["y"] = data["x"].str.token(sep=sep, n=index)
        df = otp.run(data)
        assert df["y"][0] == tokens[index]

    def test_error(self, m_session):
        t = otp.Tick(A='')
        with pytest.raises(ValueError):
            t['A'].str.token('MANY CHARACTERS')

    def test_index_out_of_range(self, m_session):
        t = otp.Tick(A='abc:def')
        t['B1'] = t['A'].str.token(':', -1)
        t['B2'] = t['A'].str.token(':', -2)
        t['B3'] = t['A'].str.token(':', -3)
        t['B4'] = t['A'].str.token(':', -4)
        t['C0'] = t['A'].str.token(':', 0)
        t['C1'] = t['A'].str.token(':', 1)
        t['C2'] = t['A'].str.token(':', 2)
        t['C3'] = t['A'].str.token(':', 3)
        df = otp.run(t)
        assert df['B1'][0] == 'def'
        assert df['B2'][0] == 'abc'
        assert df['B3'][0] == ''
        assert df['B4'][0] == ''
        assert df['C0'][0] == 'abc'
        assert df['C1'][0] == 'def'
        assert df['C2'][0] == ''
        assert df['C3'][0] == ''


class TestMatchRegexp:
    def test_default(self, m_session):
        string = "fooBAD__barBAD"
        data = otp.Tick(x=string)
        filtered_data, _ = data[data["x"].str.match(".*(BAD[_]+).*(BAD)") & (data["x"] == string)]
        assert len(otp.run(filtered_data)) == 1

    def test_add_field(self, m_session):
        string = "Hi!"
        data = otp.Tick(x=string)
        data.y = data["x"].str.match(".*match?.*")
        filtered_data, _ = data[data.y != 1]
        assert len(otp.run(filtered_data)["y"]) == 1

    def test_not_match(self, m_session):
        string = "Not match!"
        data = otp.Tick(x=string)
        filtered_data, _ = data[data["x"].str.match(".*NOT.*")]
        assert len(otp.run(filtered_data)) == 0

    def test_regexp_caseless(self, m_session):
        string = "Find SoMe here"
        data = otp.Tick(x=string)
        filtered_data_caseless, _ = data[data["x"].str.match(".*some.*", case=False)]
        assert len(otp.run(filtered_data_caseless)) == 1
        filtered_data_case, _ = data[data["x"].str.match(".*some.*", case=True)]
        assert len(otp.run(filtered_data_case)) == 0

    def test_regexp_compare(self, m_session):
        string = "...some text here..."
        data = otp.Tick(x=string)
        filtered_data, _ = data[data["x"].str.match(".*text.*") == True]  # noqa
        assert len(otp.run(filtered_data)) == 1

    def test_exception_comprasion_to_number(self, m_session):
        string = "...some text here..."
        data = otp.Tick(x=string)
        with pytest.raises(Exception):
            filtered_data, _ = data[data["x"].str.match(".*text.*") == 1]
            otp.run(filtered_data)

    def test_random_regexp(self, m_session):
        match_pattern = random_string()
        string = random_string() + match_pattern + random_string()
        data = otp.Tick(x=string)
        filtered_data, _ = data[data["x"].str.match(f".*{match_pattern}.*", case=False)]
        assert len(otp.run(filtered_data)) == 1
        mismatch_pattern = random.choice(["1", "2", "-", " "])  # by default random string contains ascii letters only
        filtered_data, _ = data[data["x"].str.match(f".*{mismatch_pattern}.*", case=False)]
        assert len(otp.run(filtered_data)) == 0


class TestLen:
    def test_random_len(self, m_session):
        strings = [random_string() for _ in range(random.randint(1, 10))]
        lens = [len(s) for s in strings]
        data = otp.Ticks(dict(x=strings))
        data["lens"] = data["x"].str.len()
        data = otp.run(data)
        assert all(data["lens"] == lens)

    def test_wrong_random_len(self, m_session):
        strings = [random_string() for _ in range(random.randint(1, 10))]
        lens = [len(s) + random.choice([-1, +1]) for s in strings]
        data = otp.Ticks(dict(x=strings))
        data["lens"] = data["x"].str.len()
        data = otp.run(data)
        assert all(data["lens"] != lens)


class TestContains:
    def test_random_contains_const(self, m_session):
        pattern = random_string(min_len=1)
        containing_string = random_string() + pattern + random_string()
        # by default random string contains ascii letters only
        non_containing_string = random_string(
            min_len=len(containing_string), max_len=len(containing_string), alphabet=[str(i) for i in range(10)]
        )
        data = otp.Ticks(dict(x=[containing_string, non_containing_string]))
        data["contains"] = data["x"].str.contains(pattern)
        data = otp.run(data)
        assert all(data["contains"].astype(int) == [1, 0])

    def test_random_contains_column(self, m_session):
        patterns = [random_string(min_len=1, alphabet=string.digits) for _ in range(1, 10)]
        contains = [random.choice([1, 0]) for _ in range(len(patterns))]
        strings = [
            random_string() + pat + random_string() if is_in else random_string(min_len=1)
            for pat, is_in in zip(patterns, contains)
        ]
        data = otp.Ticks(dict(line=strings, pattern=patterns))
        data["contains"] = data["line"].str.contains(data["pattern"])
        data = otp.run(data)
        data["contains"] = data["contains"].astype(int)  # sometimes onetick returns floats
        assert all(data["contains"] == contains)

    def test_random_where_const(self, m_session):
        const = random_string(min_len=1)
        containing_string = random_string() + const + random_string()
        # make string smaller so `const in not_containing_string == False`
        not_containing_string = random_string(max_len=len(const) - 1)
        data = otp.Ticks(dict(line=[containing_string, not_containing_string]))
        left, right = data[data["line"].str.contains(const)]
        left, right = otp.run(left), otp.run(right)
        assert len(left) == 1
        assert left["line"][0] == containing_string
        assert len(right) == 1
        assert right["line"][0] == not_containing_string

    def test_random_where_column(self, m_session):
        pattern1 = random_string()
        containing_string = random_string() + pattern1 + random_string()
        not_containing_string = random_string(max_len=50)
        # make pattern2 bigger so `pattern2 in not_containing_string == False`
        pattern2 = random_string(min_len=len(not_containing_string) + 1)
        data = otp.Ticks(dict(line=[containing_string, not_containing_string], pat=[pattern1, pattern2]))
        left, right = data[data["line"].str.contains(data["pat"])]
        left, right = otp.run(left), otp.run(right)
        assert len(left) == 1
        assert left["line"][0] == containing_string
        assert len(right) == 1
        assert right["line"][0] == not_containing_string


class TestTrims:
    def test_random_trim(self, m_session):
        pattern = random_string()
        string = random_string(alphabet=[" "]) + pattern + random_string(alphabet=[" "])
        left_string = random_string(alphabet=[" "]) + pattern
        right_string = pattern + random_string(alphabet=[" "])
        data = otp.Ticks(dict(line=[pattern, string, left_string, right_string]))
        data["line"] = data["line"].str.trim()
        data = otp.run(data)
        assert all(data["line"].str.len() == [len(pattern)] * len(data))

    def test_random_ltrim(self, m_session):
        pattern = random_string(min_len=1)
        r_spaces = random_string(alphabet=[" "])
        string = random_string(alphabet=[" "]) + pattern + r_spaces
        left_string = random_string(alphabet=[" "]) + pattern
        right_string = pattern + r_spaces
        data = otp.Ticks(dict(line=[pattern, string, left_string, right_string]))
        data["line"] = data["line"].str.ltrim()
        data = otp.run(data)
        assert all(
            data["line"].str.len()
            == [len(pattern), len(pattern) + len(r_spaces), len(pattern), len(pattern) + len(r_spaces)]
        )

    def test_random_rtrim(self, m_session):
        pattern = random_string(min_len=1)
        l_spaces = random_string(alphabet=[" "])
        string = l_spaces + pattern + random_string(alphabet=[" "])
        left_string = l_spaces + pattern
        right_string = pattern + random_string(alphabet=[" "])
        data = otp.Ticks(dict(line=[pattern, string, left_string, right_string]))
        data["line"] = data["line"].str.rtrim()
        data = otp.run(data)
        assert all(
            data["line"].str.len()
            == [len(pattern), len(pattern) + len(l_spaces), len(pattern) + len(l_spaces), len(pattern)]
        )


class TestCaseFunctions:
    def test_random_lower(self, m_session):
        string = random_string()
        data = otp.Ticks(dict(line=[string]))
        data["line"] = data["line"].str.lower()
        assert issubclass(data["line"].dtype, str)
        data = otp.run(data)
        assert all(data["line"].str.islower() | (data["line"] == ""))

    def test_random_upper(self, m_session):
        string = random_string()
        data = otp.Ticks(dict(line=[string]))
        data["line"] = data["line"].str.upper()
        assert issubclass(data["line"].dtype, str)
        data = otp.run(data)
        assert all(data["line"].str.isupper() | (data["line"] == ""))


class TestReplace:
    def _get_replace_data(self, many):
        r_string = partial(random_string, max_len=40)
        n = random.randint(1, 10)
        # form n random part of m random string for joining them later
        strings = [[r_string(alphabet=string.digits) for _ in range(random.randint(1, 10))] for _ in range(n)]
        if many:
            pat = [r_string(min_len=1) for _ in range(n)]
            repl = [r_string() for _ in range(n)]
            strings = [p.join(s) for s, p in zip(strings, pat)]
            # OneTick drops trailing characters if length of string is longer than the biggest one during ticks creating
            strings[0] = otp.string[19 * 40](strings[0])
            expected = [s.replace(p, r) for s, p, r in zip(strings, pat, repl)]
            data = otp.Ticks(dict(x=strings, pat=pat, repl=repl))
        else:
            pat = r_string(min_len=1)
            repl = r_string()
            strings = [pat.join(s) for s in strings]
            # OneTick drops trailing characters if length of string is longer than the biggest one during ticks creating
            strings[0] = otp.string[19 * 40](strings[0])
            expected = [s.replace(pat, repl) for s in strings]
            data = otp.Ticks(dict(x=strings))
        return data, pat, repl, expected

    def test_random_const(self, m_session):
        data, pat, repl, expected = self._get_replace_data(many=False)
        data["x"] = data["x"].str.replace(pat, repl)
        data = otp.run(data)
        assert all(data["x"] == expected)

    def test_random_columns(self, m_session):
        data, _, _, expected = self._get_replace_data(many=True)
        data["x"] = data["x"].str.replace(data["pat"], data["repl"])
        data = otp.run(data)
        assert all(data["x"] == expected)


class TestRegexReplace:
    def _get_random_replace_params(self):
        regex = random_regex()
        caseless, replace_every = random.choices([True, False], k=2)
        # https://pandas.pydata.org/pandas-docs/version/0.25/reference/api/pandas.Series.str.replace.html#pandas.Series.str.replace
        case = not caseless  # pandas case param has opposite meaning
        n = -1 if replace_every else 1  # pandas support number of replaces while OneTick one global flag
        return regex, caseless, case, replace_every, n

    def test_random_const(self, m_session):
        pat, caseless, case, replace_every, n = self._get_random_replace_params()
        repl = random_string(max_len=10)
        strings = [random_string() for _ in range(random.randint(10, 100))]
        expected = pd.Series(strings, name="x")
        expected = expected.str.replace(pat, repl, case=case, regex=True, n=n)
        # OneTick drops trailing characters if length of string is longer than the biggest one during ticks creating
        strings[0] = otp.string[1000](strings[0])
        data = otp.Ticks(dict(x=strings))
        data["x"] = data["x"].str.regex_replace(pat, repl, replace_every=replace_every, caseless=caseless)
        data = otp.run(data)
        assert all(data["x"] == expected)


class TestFind:
    def test_random_const_presented(self, m_session):
        sub = random_string(min_len=1)
        beginnings = [random_string() for _ in range(random.randint(1, 10))]
        expected = [len(beg) for beg in beginnings]
        data = otp.Ticks(dict(x=[beg + sub + random_string() for beg in beginnings]))
        data["y"] = data["x"].str.find(sub)
        data = otp.run(data)
        assert all(data["y"] <= expected)  # <= because sub can be presented in random beginning

    def test_random_column_presented(self, m_session):
        subs = [random_string(min_len=1) for _ in range(random.randint(1, 10))]
        beginnings = [random_string() for _ in subs]
        expected = [len(beg) for beg in beginnings]
        data = otp.Ticks(dict(x=[beg + sub + random_string() for beg, sub in zip(beginnings, subs)], sub=subs))
        data["y"] = data["x"].str.find(data["sub"])
        data = otp.run(data)
        assert all(data["y"] <= expected)

    def test_random_const_not_presented(self, m_session):
        lines = [random_string() for _ in range(random.randint(1, 10))]
        bigger = generate_bigger_strings(lines, only_one=True)
        other_alphabet = generate_different_alphabet_strings(lines, only_one=True)
        data = otp.Ticks(dict(x=lines))
        data["bigger"] = data["x"].str.find(bigger)
        data["other_alphabet"] = data["x"].str.find(other_alphabet)
        data = otp.run(data)
        assert all(data["bigger"] == [-1] * len(lines))
        assert all(data["other_alphabet"] == [-1] * len(lines))

    def test_random_column_not_presented(self, m_session):
        lines = [random_string() for _ in range(random.randint(1, 10))]
        # form bunch of substring which is longer than existed ones, so they won't be presented.
        bigger = generate_bigger_strings(lines, only_one=False)
        other_alphabet = generate_different_alphabet_strings(lines, only_one=False)
        data = otp.Ticks(dict(x=lines, bigger=bigger, other_alphabet=other_alphabet))
        data["bigger"] = data["x"].str.find(data["bigger"])
        data["other_alphabet"] = data["x"].str.find(data["other_alphabet"])
        data = otp.run(data)
        assert all(data["bigger"] == [-1] * len(lines))
        assert all(data["other_alphabet"] == [-1] * len(lines))


@pytest.mark.skipif(not is_repeat_with_field_name_works_correctly(), reason="REPEAT EP on fields is broken")
class TestRepeat:
    old_column_name = "x"
    new_column_name = "y"
    int_column_name = "n"

    @contextmanager
    def _repeat_test_init_and_check(self, new_column_name, by_const=True):
        strings = [random_string() for _ in range(random.randint(1, 10))]
        max_len = max(64, max(len(s) for s in strings))  # 64 is default length
        if by_const:
            n = random.randint(0, 10)
            data = otp.Ticks({self.old_column_name: strings})
        else:
            n = [random.randint(0, 10) for _ in strings]
            data = otp.Ticks({self.old_column_name: strings, self.int_column_name: n})
        yield data, n
        data = otp.run(data)
        # onetick doesn't increase the length of column's strings
        expected = [(s * n)[:max_len] for s in strings] if by_const else [(s * i)[:max_len] for s, i in zip(strings, n)]
        assert all(data[new_column_name] == expected)

    def test_mul_right_const(self, m_session):
        with self._repeat_test_init_and_check(self.new_column_name) as (data, n):
            data[self.new_column_name] = data[self.old_column_name] * n

    def test_mul_left_const(self, m_session):
        with self._repeat_test_init_and_check(self.new_column_name) as (data, n):
            data[self.new_column_name] = n * data[self.old_column_name]

    def test_repeat_const(self, m_session):
        with self._repeat_test_init_and_check(self.new_column_name) as (data, n):
            data[self.new_column_name] = data[self.old_column_name].str.repeat(n)

    def test_mul_left_column(self, m_session):
        with self._repeat_test_init_and_check(self.new_column_name, by_const=False) as (data, n):
            data[self.new_column_name] = data[self.old_column_name] * data[self.int_column_name]

    def test_mul_right_column(self, m_session):
        with self._repeat_test_init_and_check(self.new_column_name, by_const=False) as (data, n):
            data[self.new_column_name] = data[self.int_column_name] * data[self.old_column_name]

    def test_repeat_column(self, m_session):
        with self._repeat_test_init_and_check(self.new_column_name, by_const=False) as (data, n):
            data[self.new_column_name] = data[self.old_column_name].str.repeat(data[self.int_column_name])

    @pytest.mark.parametrize("argument", [random_string(), random.random()])
    def test_raise_error_on_invalid_type(self, argument, m_session):
        with pytest.raises(TypeError):
            with self._repeat_test_init_and_check(self.new_column_name) as (data, n):
                data[self.new_column_name] = data[self.old_column_name] * argument
        with pytest.raises(TypeError):
            with self._repeat_test_init_and_check(self.new_column_name) as (data, n):
                data[self.new_column_name] = argument * data[self.old_column_name]


class TestExtract:
    def test_extract_digits(self, m_session):
        pat = r"\d+"
        strings = [(random_string(), random_string()) for _ in range(random.randint(1, 10))]
        digit_strings = generate_different_alphabet_strings(strings, new_alphabet=string.digits, only_one=False)
        strings = [left + digits + right for digits, (left, right) in zip(digit_strings, strings)]
        data = otp.Ticks(dict(x=strings))
        data["only_digits"] = data["x"].str.extract(pat, rewrite=r"\0")
        data = otp.run(data)
        assert all(data["only_digits"].str.isnumeric() | data["only_digits"].str.isspace())


class TestSubstr:
    def test_random_substr_with_rtrim_by_const(self, m_session):
        ending = random_string(min_len=1)
        n_bytes = len(ending)
        start_index = -n_bytes
        strings = [random_string() + ending for _ in range(random.randint(1, 10))]
        data = otp.Ticks(dict(x=strings))
        data["end1"] = data["x"].str.substr(start_index, n_bytes, rtrim=True)
        data = otp.run(data)
        assert all(data["end1"] == [ending] * len(strings))

    def test_random_substr_by_columns(self, m_session):
        strings = [(random_string(), random_string(), random_string()) for _ in range(random.randint(1, 10))]
        expected = [s[1] for s in strings]  # we will get center part
        start_index = [len(s[0]) for s in strings]
        n_bytes = [len(s[1]) for s in strings]
        strings = [left + center + right for left, center, right in strings]
        data = otp.Ticks(dict(x=strings, start_index=start_index, n=n_bytes))
        data["center"] = data["x"].str.substr(data["start_index"], data["n"])
        data = otp.run(data)
        assert all(data["center"] == expected)

    def test_default_n_bytes(self, m_session):
        """ Check that n_bytes could be omitted """
        data = otp.Ticks(X=['abcdef'])
        data['Y'] = data['X'].str.substr(2)
        df = otp.run(data)
        assert all(df['Y'] == ['cdef'])


class TestQuotes:
    @pytest.mark.parametrize('quote', ["'", '"', """ ----> " ' <----- """])
    def test_quotes(self, m_session, quote):
        t = otp.Tick(Q=quote)
        t['A'] = t['Q'].str.replace(quote, '')
        t['B'] = t['Q'].str.replace('_', quote)
        t['C'] = t['Q'].str.regex_replace(quote, '')
        t['D'] = t['Q'].str.regex_replace('_', quote)
        t['E'] = t['Q'].str.contains(quote)
        t['F'] = t['Q'].str.find(quote)
        t['G'] = t['Q'].str.match(quote)
        if len(quote) == 1:
            t['H'] = t['Q'].str.token(quote)
        t['I'] = t['Q'].str.extract(quote)
        t['J'] = t['Q'].str.to_datetime(quote)
        df = otp.run(t)
        assert df['A'][0] == ''
        assert df['B'][0] == quote
        assert df['C'][0] == ''
        assert df['D'][0] == quote
        assert df['E'][0] == 1
        assert df['F'][0] == 0
        assert df['G'][0] == 1
        if len(quote) == 1:
            assert df['H'][0] == ''
        assert df['I'][0] == quote
        assert df['J'][0] == pd.Timestamp(0, tz=otp.config['tz']).replace(tzinfo=None)


class TestSlice:
    @pytest.mark.parametrize("start,ans", [
        (0, '12345678'),
        (1, '2345678'),
        (5, '678'),
        (10, ''),
        (-1, '8'),
        (-2, '78'),
        (-5, '45678'),
        (-10, '12345678'),
    ])
    def test_start(self, m_session, start, ans):
        data = otp.Tick(X='12345678')
        data['X_SLICE_1'] = data['X'].str.slice(start=start)
        data['X_SLICE_2'] = data['X'].str[start:]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == ans
        assert df['X_SLICE_2'][0] == ans

    def test_start_same_chars(self, m_session):
        data = otp.Tick(X='a' * 10)
        data['X_SLICE_1'] = data['X'].str.slice(start=1)
        data['X_SLICE_2'] = data['X'].str[1:]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == 'a' * 9
        assert df['X_SLICE_2'][0] == 'a' * 9

    @pytest.mark.parametrize("start", [-2, 7])
    def test_start_no_trim(self, m_session, start):
        data = otp.Tick(X='12345678 ')
        data['X_SLICE_1'] = data['X'].str.slice(start=start)
        data['X_SLICE_2'] = data['X'].str[start:]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == '8 '
        assert df['X_SLICE_2'][0] == '8 '

    @pytest.mark.parametrize("stop,ans", [
        (0, ''),
        (1, '1'),
        (5, '12345'),
        (10, '12345678'),
        (-1, '1234567'),
        (-2, '123456'),
        (-5, '123'),
        (-10, ''),
    ])
    def test_stop(self, m_session, stop, ans):
        data = otp.Tick(X='12345678')
        data['X_SLICE_1'] = data['X'].str.slice(stop=stop)
        data['X_SLICE_2'] = data['X'].str[:stop]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == ans
        assert df['X_SLICE_2'][0] == ans

    def test_stop_same_chars(self, m_session):
        data = otp.Tick(X='a' * 10)
        data['X_SLICE_1'] = data['X'].str.slice(stop=9)
        data['X_SLICE_2'] = data['X'].str[:9]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == 'a' * 9
        assert df['X_SLICE_2'][0] == 'a' * 9

    @pytest.mark.parametrize("stop", [2, -7])
    def test_stop_no_trim(self, m_session, stop):
        data = otp.Tick(X=' 12345678')
        data['X_SLICE_1'] = data['X'].str.slice(stop=stop)
        data['X_SLICE_2'] = data['X'].str[:stop]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == ' 1'
        assert df['X_SLICE_2'][0] == ' 1'

    @pytest.mark.parametrize("start,stop,ans", [
        (0, 1, '1'),
        (1, 2, '2'),
        (1, 1, ''),
        (7, 8, '8'),
        (8, 11, ''),
        (2, 7, '34567'),
        (-1, 8, '8'),
        (-2, -1, '7'),
        (-10, -1, '1234567'),
        (-3, -2, '6'),
        (-2, -2, ''),
        (-2, -3, ''),
        (-20, -10, ''),
        (1, -1, '234567'),
    ])
    def test_start_stop(self, m_session, start, stop, ans):
        data = otp.Tick(X='12345678')
        data['X_SLICE_1'] = data['X'].str.slice(start=start, stop=stop)
        data['X_SLICE_2'] = data['X'].str[start:stop]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == ans
        assert df['X_SLICE_2'][0] == ans

    def test_start_stop_same_chars(self, m_session):
        data = otp.Tick(X='a' * 10)
        data['X_SLICE_1'] = data['X'].str.slice(start=0, stop=9)
        data['X_SLICE_2'] = data['X'].str[0:9]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == 'a' * 9
        assert df['X_SLICE_2'][0] == 'a' * 9

    @pytest.mark.parametrize("start,stop", [(1, 11), (-11, -1)])
    def test_start_stop_no_trim(self, m_session, start, stop):
        data = otp.Tick(X='  12345678  ')
        data['X_SLICE_1'] = data['X'].str.slice(start=start, stop=stop)
        data['X_SLICE_2'] = data['X'].str[start:stop]
        df = otp.run(data)
        assert df['X_SLICE_1'][0] == ' 12345678 '
        assert df['X_SLICE_2'][0] == ' 12345678 '


def test_replace_parameters(m_session):
    from onetick.py.functions import _add_node_name_prefix_to_columns_in_operation
    t = otp.Tick(AAA='a', BBB='b', CCC='c', XXX=1, YYY=2)
    t.node_name('PREFIX')

    ops = [
        t['AAA'].str.to_datetime(t['BBB'], t['CCC']),
        t['AAA'].str.token(t['BBB'], t['XXX']),
        t['AAA'].str.match(t['BBB']),
        t['AAA'].str.len(),
        t['AAA'].str.contains(t['BBB']),
        t['AAA'].str.trim(),
        t['AAA'].str.rtrim(),
        t['AAA'].str.ltrim(),
        t['AAA'].str.lower(),
        t['AAA'].str.upper(),
        t['AAA'].str.replace(t['BBB'], t['CCC']),
        t['AAA'].str.regex_replace(t['BBB'], t['CCC']),
        t['AAA'].str.find(t['BBB'], t['XXX']),
        t['AAA'].str.repeat(t['XXX']),
        t['AAA'].str.extract(t['BBB'], t['CCC']),
        t['AAA'].str.substr(t['BBB'], t['XXX']),
        t['AAA'].str.get(t['XXX']),
        t['AAA'].str.concat(t['BBB']),
        t['AAA'].str.insert(t['XXX'], t['YYY'], t['BBB']),
        t['AAA'].str.first(t['XXX']),
        t['AAA'].str.last(t['XXX']),
        t['AAA'].str.startswith(t['BBB']),
        t['AAA'].str.endswith(t['BBB']),
        t['AAA'].str.slice(t['XXX'], t['YYY']),
        t['AAA'].str.like('%'),
        t['AAA'].str.ilike('%'),
    ]
    for op in ops:
        str_op = str(op)
        str_replaced_op = str_op[:]
        for column in t.schema:
            str_replaced_op = str_replaced_op.replace(column, f'PREFIX.{column}')
        str_node_name_prefix_op = str(_add_node_name_prefix_to_columns_in_operation(op, t))
        assert str_replaced_op == str_node_name_prefix_op
