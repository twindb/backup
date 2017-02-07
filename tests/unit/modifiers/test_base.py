import mock

from twindb_backup.modifiers.base import Modifier


def test_modifier(tmpdir):
    input_stream = tmpdir.join('in.txt')
    f_in = open(str(input_stream), 'w+')
    Modifier(f_in)


def test_modifier_get_stream(input_file):
    with open(str(input_file), 'r') as f:
        m = Modifier(f)
        with m.get_stream() as m_f:
            assert m_f.read() == 'foo bar'


def test_modifier_get_stream_calls_callback(input_file):
    mock_func = mock.Mock()
    with open(str(input_file), 'r') as f:
        m = Modifier(f, mock_func, foo='bar', aaa='bbb')
        with m.get_stream() as m_f:
            assert m_f.read() == 'foo bar'
        mock_func.assert_called_once_with(foo='bar', aaa='bbb')
