import re


def get_field(doc: str, name: str):
    match = re.search(':{}: (.*)$'.format(name), doc, re.IGNORECASE | re.MULTILINE)
    if match:
        return match.group(1).strip()


class TestVersion:
    def test_version_is_consistent(self):
        from .. import VERSION

        with open('README.rst') as f:
            readme = f.read()

            version = get_field(readme, 'version')
            version = tuple(map(int, version.split('.')))

        assert VERSION == version
