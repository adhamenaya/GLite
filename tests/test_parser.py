from GLite.glite import GLiteTokenizer,GLiteParser

def test_parse_simple_match():
    query = "MATCH (a:Person {name: 'Alice'}) RETURN a"
    tokenizer = GLiteTokenizer()
    tokens = list(tokenizer.tokenize(query))
    parser = GLiteParser(tokens)
    pattern = parser.parse_path_pattern()
    assert pattern.expr.nodes[0].filler._type == "Person"
