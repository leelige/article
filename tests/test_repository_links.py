import unittest

from main import clean_repository_url


class RepositoryLinkTest(unittest.TestCase):
    def test_extracts_only_github_repository_from_summary(self):
        summary = (
            "Code: https://github.com/FAU-LMS/MHPM and trained four "
            "different classifiers."
        )
        self.assertEqual(
            clean_repository_url(summary),
            "https://github.com/FAU-LMS/MHPM",
        )

    def test_returns_null_without_repository(self):
        self.assertEqual(clean_repository_url("No code is available."), "null")

    def test_keeps_standalone_non_github_repository(self):
        self.assertEqual(
            clean_repository_url("https://gitlab.com/example/project"),
            "https://gitlab.com/example/project",
        )


if __name__ == "__main__":
    unittest.main()
