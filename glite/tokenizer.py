import re
from collections import namedtuple

Token = namedtuple("Token", ["type", "value"])

class GLiteTokenizer:
    TOKEN_REGEX = r"""
        (?P<NOT_IN>\bNOT_IN\b) |
        (?P<MATCH>\bMATCH\b) |
        (?P<RETURN>\bRETURN\b) |
        (?P<DISTINCT>\bDISTINCT\b) |
        (?P<AS>\bAS\b) |
        (?P<WHERE>\bWHERE\b) |
        (?P<IN>\bIN\b) |
        (?P<NEAR>\bNEAR\b) |
        (?P<SYS_VAR>\bSYS_VAR\b) |
        (?P<IN_VAR>\bIN_VAR\b) |
        (?P<ARROW_LEFT><-) |
        (?P<ARROW_RIGHT>->) |
        (?P<NEQ>!=) |
        (?P<LTE><=) |
        (?P<GTE>>=) |
        (?P<EQ>=) |
        (?P<LT><) |
        (?P<GT>>) |
        (?P<DASH>-) |
        (?P<LPAREN>\() |
        (?P<RPAREN>\)) |
        (?P<LBRACK>\[) |
        (?P<RBRACK>\]) |
        (?P<LBRACE>\{) |
        (?P<RBRACE>\}) |
        (?P<COLON>:) |
        (?P<COMMA>,) |
        (?P<DOT>\.) |
        (?P<PARAM>\$\{[a-zA-Z_][a-zA-Z0-9_]*\}) |
        (?P<NUMBER>\d+(\.\d+)?) |
        (?P<STRING>"[^"]*"|'[^']*') |
        (?P<IDENT>[a-zA-Z_][a-zA-Z0-9_]*) |
        (?P<WHITESPACE>\s+)
    """

    token_pattern = re.compile(TOKEN_REGEX, re.VERBOSE | re.IGNORECASE)

    def tokenize(self, input_string):
        """Yield tokens one by one, skipping whitespace."""
        for match in self.token_pattern.finditer(input_string):
            kind = match.lastgroup
            value = match.group()
            if kind != "WHITESPACE":
                yield Token(kind, value)
