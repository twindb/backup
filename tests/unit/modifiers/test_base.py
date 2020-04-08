from twindb_backup.modifiers.base import Modifier


def test_modifier(tmpdir):
    input_stream = tmpdir.join('in.txt')
    f_in = open(str(input_stream), 'w+')
    m = Modifier(f_in)
    assert m.input == f_in


def test_modifier_get_stream(input_file):
    with open(str(input_file), 'r') as f:
        m = Modifier(f)
        with m.get_stream() as m_f:
            assert m_f.read().decode("utf-8") == "foo bar"
