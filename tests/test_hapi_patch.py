import pytest
from github import UnknownObjectException

from hapi.pipelines.utilities.hapi_patch import HAPIPatch, HAPIPatchError


class MockValue:
    def __init__(self, value):
        self.value = value


class MockFile:
    def __init__(self, type, path):
        self.type = type
        self.path = path


class MockRepo:
    result = {}
    variables = {}
    files = []

    @classmethod
    def get_variable(cls, name):
        result = cls.variables.get(name)
        if result is not None:
            return result
        raise UnknownObjectException(-1)

    @classmethod
    def create_variable(cls, name, value):
        cls.variables[name] = MockValue(value)

    @staticmethod
    def delete_variable(name):
        pass

    @classmethod
    def get_contents(cls, path):
        return cls.files

    @classmethod
    def create_file(cls, filename, message, content, **kwargs):
        cls.result[filename] = {"message": message, "content": content}


class MockGithub:
    def __init__(self, auth):
        pass

    def get_repo(self, hapi_repo):
        return MockRepo()

    @staticmethod
    def close():
        pass


class TestHAPIPatch:
    def test_hapi_patch(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "test")
        with HAPIPatch("test", githubclass=MockGithub) as hapi_patch:
            val = hapi_patch.get_sequence_number()
            assert val == 1
            patch = {"test": [{"key": "1"}, {"key": 2}]}
            hapi_patch.create("suffix", patch)
            assert MockRepo.result == {
                "hapi_patch_1_suffix.json": {
                    "content": '{"test":[{"key":"1"},{"key":2}]}',
                    "message": "Creating hapi_patch_1_suffix.json",
                }
            }
            val = hapi_patch.get_sequence_number()
            assert val == 2
            patch = {"test": [{"key": 2}, {"key": "3"}]}
            hapi_patch.create("suffix", patch)
            assert MockRepo.result == {
                "hapi_patch_1_suffix.json": {
                    "content": '{"test":[{"key":"1"},{"key":2}]}',
                    "message": "Creating hapi_patch_1_suffix.json",
                },
                "hapi_patch_2_suffix.json": {
                    "content": '{"test":[{"key":2},{"key":"3"}]}',
                    "message": "Creating hapi_patch_2_suffix.json",
                },
            }
            val = hapi_patch.get_sequence_number()
            assert val == 3

        MockRepo.variables = {"LOCK": MockValue("LOCKED")}
        with pytest.raises(HAPIPatchError):
            HAPIPatch("test", 1, 0, MockGithub)

        MockRepo.variables = {}
        MockRepo.files = [
            MockFile("file", "hapi_patch_1_hno_afg.json"),
            MockFile("dir", "test_dir"),
            MockFile("file", "hapi_patch_2_hno_afg.json"),
        ]
        with HAPIPatch("test", githubclass=MockGithub) as hapi_patch:
            val = hapi_patch.get_sequence_number()
            assert val == 3
        monkeypatch.delenv("GITHUB_TOKEN")
