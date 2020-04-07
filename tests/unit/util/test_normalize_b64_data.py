from base64 import b64decode

import pytest

from twindb_backup.util import normalize_b64_data


@pytest.mark.parametrize('data, expected', [
    (
        "W215c3FsZF0KdXNlciAgICAgICAgICAgID0gcm9vdApwaWQtZmlsZSAgICAgICAgPS"
        "AvdmFyL3J1bi9teXNxbGQvbXlzcWxkLnBpZApzb2NrZXQgICAgICAgICAgPSAvdmFy"
        "L3J1bi9teXNxbGQvbXlzcWxkLnNvY2sKZGF0YWRpciAgICAgICAgID0gL3Zhci9saW"
        "IvbXlzcWwKbG9nLWVycm9yICAgICAgID0gL3Zhci9sb2cvbXlzcWwvZXJyb3IubG9n"
        "CiMgQnkgZGVmYXVsdCB3ZSBvbmx5IGFjY2VwdCBjb25uZWN0aW9ucyBmcm9tIGxvY2"
        "FsaG9zdApiaW5kLWFkZHJlc3MgICAgPSAwLjAuMC4wCgojIERpc2FibGluZyBzeW1i"
        "b2xpYy1saW5rcyBpcyByZWNvbW1lbmRlZCB0byBwcmV2ZW50IGFzc29ydGVkIHNlY3"
        "VyaXR5IHJpc2tzCnN5bWJvbGljLWxpbmtzICA9IDAKCnNlcnZlcl9pZCAgICAgICA9"
        "IDEwMApsb2ctYmluICAgICAgICAgPSBteXNxbC1iaW4KbG9nLXNsYXZlLXVwZGF0ZX"
        "MK",
        "[mysqld]\n"
        "user            = root\n"
        "pid-file        = /var/run/mysqld/mysqld.pid\n"
        "socket          = /var/run/mysqld/mysqld.sock\n"
        "datadir         = /var/lib/mysql\n"
        "log-error       = /var/log/mysql/error.log\n"
        "# By default we only accept connections from localhost\n"
        "bind-address    = 0.0.0.0\n"
        "\n"
        "# Disabling symbolic-links is recommended to prevent assorted security risks\n"
        "symbolic-links  = 0\n"
        "\n"
        "server_id       = 100\n"
        "log-bin         = mysql-bin\n"
        "log-slave-updates\n",
    ),
    (
        "IyBDb3B5cmlnaHQgKGMpIDIwMTYsIE9yYWNsZSBhbmQvb3IgaXRzIGFmZmlsaWF0ZXMuI"
        "EFsbCByaWdodHMgcmVzZXJ2ZWQuCiMKIyBUaGlzIHByb2dyYW0gaXMgZnJlZSBzb2Z0d2"
        "FyZTsgeW91IGNhbiByZWRpc3RyaWJ1dGUgaXQgYW5kL29yIG1vZGlmeQojIGl0IHVuZGV"
        "yIHRoZSB0ZXJtcyBvZiB0aGUgR05VIEdlbmVyYWwgUHVibGljIExpY2Vuc2UgYXMgcHVi"
        "bGlzaGVkIGJ5CiMgdGhlIEZyZWUgU29mdHdhcmUgRm91bmRhdGlvbjsgdmVyc2lvbiAyI"
        "G9mIHRoZSBMaWNlbnNlLgojCiMgVGhpcyBwcm9ncmFtIGlzIGRpc3RyaWJ1dGVkIGluIH"
        "RoZSBob3BlIHRoYXQgaXQgd2lsbCBiZSB1c2VmdWwsCiMgYnV0IFdJVEhPVVQgQU5ZIFd"
        "BUlJBTlRZOyB3aXRob3V0IGV2ZW4gdGhlIGltcGxpZWQgd2FycmFudHkgb2YKIyBNRVJD"
        "SEFOVEFCSUxJVFkgb3IgRklUTkVTUyBGT1IgQSBQQVJUSUNVTEFSIFBVUlBPU0UuICBTZ"
        "WUgdGhlCiMgR05VIEdlbmVyYWwgUHVibGljIExpY2Vuc2UgZm9yIG1vcmUgZGV0YWlscy4"
        "KIwojIFlvdSBzaG91bGQgaGF2ZSByZWNlaXZlZCBhIGNvcHkgb2YgdGhlIEdOVSBHZW5lc"
        "mFsIFB1YmxpYyBMaWNlbnNlCiMgYWxvbmcgd2l0aCB0aGlzIHByb2dyYW07IGlmIG5vdCw"
        "gd3JpdGUgdG8gdGhlIEZyZWUgU29mdHdhcmUKIyBGb3VuZGF0aW9uLCBJbmMuLCA1MSBGc"
        "mFua2xpbiBTdCwgRmlmdGggRmxvb3IsIEJvc3RvbiwgTUEgIDAyMTEwLTEzMDEgVVNBCgoh"
        "aW5jbHVkZWRpciAvZXRjL215c3FsL2NvbmYuZC8KIWluY2x1ZGVkaXIgL2V0Yy9teXNxbC9"
        "teXNxbC5jb25mLmQvCg==",
"# Copyright (c) 2016, Oracle and/or its affiliates. All rights reserved.\n"
"#\n"
"# This program is free software; you can redistribute it and/or modify\n"
"# it under the terms of the GNU General Public License as published by\n"
"# the Free Software Foundation; version 2 of the License.\n"
"#\n"
"# This program is distributed in the hope that it will be useful,\n"
"# but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
"# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
"# GNU General Public License for more details.\n"
"#\n"
"# You should have received a copy of the GNU General Public License\n"
"# along with this program; if not, write to the Free Software\n"
"# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA\n"
"\n"
"!includedir /etc/mysql/conf.d/\n"
"!includedir /etc/mysql/mysql.conf.d/\n",
    )
])
def test_b64_normalize(data, expected):
    result = b64decode(normalize_b64_data(data)).decode("utf-8")
    assert result == expected
