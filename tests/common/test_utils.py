import unittest
from xetra_jobs.common.utils import list_dates


class TestUtils(unittest.TestCase):
    """
    test utility functions
    """

    def test_list_dates(self):
        """
        list_dates should return last Friday given Monday or weekend (it's the previous workday)
        """

        monday = "2021-09-13"
        sunday = "2021-09-12"
        saturday = "2021-09-11"
        friday = "2021-09-10"
        date_format = "%Y-%m-%d"

        self.assertEqual([friday, monday], list_dates(monday, date_format))
        self.assertEqual([friday, sunday], list_dates(sunday, date_format))
        self.assertEqual([friday, saturday], list_dates(saturday, date_format))


if __name__ == '__main__':
    unittest.main()
