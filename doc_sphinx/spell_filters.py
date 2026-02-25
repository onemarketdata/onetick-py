import enchant.tokenize


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
