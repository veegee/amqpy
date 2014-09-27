class TestVersion:
    def test_version_is_consistent(self):
        from .. import VERSION

        with open('README.md') as f:
            readme = f.read().split('\n')
            version_list = readme[3].split(':')[1].strip().split('.')
            version_list = [int(i) for i in version_list]
            readme_version = tuple(version_list)

        assert VERSION == readme_version

