import pytest

import onetick.py as otp


class TestCharacterPresent:
    def run(self, params, result, columns_as_str=True):
        data = otp.Ticks(
            X=[1, 2, 3, 4, 5, 6, 7, 8],
            F=['T1', 'T2', 'A1', 'A2', 'B1', 'A3', 'B2', 'A4'],
            A=['1', '2', '3', '4A', 'B', '3', 'F', ''],
        )

        if not columns_as_str:
            if 'field' in params:
                params['field'] = data[params['field']]

            if 'characters_field' in params:
                params['characters_field'] = data[params['characters_field']]

        data = data.character_present(**params)
        df = otp.run(data)
        assert list(df['X']) == result

    @pytest.mark.parametrize('columns_as_str', [True, False])
    def test_character_present(self, columns_as_str, session):
        params = {
            'field': 'F',
            'characters': 'AB',
        }

        self.run(params, [3, 4, 5, 6, 7, 8], columns_as_str=columns_as_str)

    def test_character_present_list(self, session):
        params = {
            'field': 'F',
            'characters': ['A', 'B'],
        }

        self.run(params, [3, 4, 5, 6, 7, 8])

    @pytest.mark.skipif(
        not otp.compatibility.is_character_present_characters_field_fixed(),
        reason="CHARACTER_FIELD parameter issues on OneTick side for used OneTick version",
    )
    @pytest.mark.parametrize('columns_as_str', [True, False])
    def test_character_present_character_field(self, columns_as_str, session):
        params = {
            'field': 'F',
            'characters': '2',
            'characters_field': 'A',
        }

        self.run(params, [1, 2, 4, 5, 6, 7], columns_as_str=columns_as_str)

    def test_missing_columns(self, session):
        with pytest.raises(ValueError):
            self.run({'field': 'TEST', 'characters': 'A'}, [])

        with pytest.raises(ValueError):
            self.run({'field': 'A', 'characters': 'A', 'characters_field': 'TEST'}, [])

    def test_wrong_types(self, session):
        data = otp.Ticks(X=[1, 2, 3], T=['a', 'b', 'c'])

        with pytest.raises(TypeError):
            _ = data.character_present(field='X', characters='a')

        with pytest.raises(TypeError):
            _ = data.character_present(field='T', characters='a', characters_field='X')

    def test_otp_string(self, session):
        data = otp.Ticks(X=[otp.string[1024]("a"), otp.string[1024]("b")])
        data = data.character_present(field='X', characters='a')
        df = otp.run(data)
        assert list(df['X']) == ['a']
