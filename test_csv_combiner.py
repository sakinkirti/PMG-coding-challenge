import unittest
import pandas as pd
import numpy as np

from csv_combiner import csvCombiner as CC
from csv_combiner import NoFilesError
from csv_combiner import FileNotFoundError
from csv_combiner import FileSizeError
from csv_combiner import NoValueError

class test_csvCombiner(unittest.TestCase):
    """
    class containing testing methods to test the various functions of the csvCombiner
    tests for the following methods

    csvCombiner.combine()
    - 2+ files given
    - only 1 file given
    - files with differing column names
    - failure conditions are hit (file does not exist or no data in the file
    
    csvCombiner.print()
    - when nothing is stored
    - when a df is stored
    """

    def test_combine(self):
        """
        tests the combine() method of the csvCombiner
        """

        # define files
        proper_files = ["test_data/accessories.csv", "test_data/clothing.csv", "test_data/household_cleaners.csv"]
        combiner = CC(files=proper_files, chunk_size=10000)

        # 2+ files given
        combiner.combine()
        self.assertEqual(len(combiner.combined.columns), 3)
        self.assertEqual(len(combiner.combined), len(pd.read_csv("test_data/accessories.csv"))+len(pd.read_csv("test_data/clothing.csv"))+len(pd.read_csv("test_data/household_cleaners.csv")))
        self.assertEqual(len(np.unique(combiner.combined["filename"])), 3)

        # only 1 file given
        combiner.files = ["test_data/accessories.csv"]
        combiner.combine()
        self.assertEqual(len(combiner.combined.columns), 3)
        self.assertEqual(len(combiner.combined), len(pd.read_csv("test_data/accessories.csv")))
        self.assertEqual(np.unique(combiner.combined["filename"]), "accessories.csv")

        # files with different column names
        combiner.files = ["test_data/clothing.csv", "test_data/clothing_cost.csv"]
        combiner.combine()
        self.assertEqual(len(combiner.combined), len(pd.read_csv("test_data/clothing.csv"))+len(pd.read_csv("test_data/clothing_cost.csv")))
        self.assertEqual(len(combiner.combined.columns), 4)

        # test no files input
        combiner.files = []
        self.assertRaises(NoFilesError, combiner.combine)

        # non-existent file name input
        combiner.files = ["test_data/house.csv", "test_data/accessories.csv", "test_data/clothing.csv", "test_data/household_cleaners.csv"]
        self.assertRaises(FileNotFoundError, combiner.combine)

        # file with no data
        combiner.files = ["test_data/empty_file.csv", "test_data/accessories.csv", "test_data/clothing.csv", "test_data/household_cleaners.csv"]
        self.assertRaises(FileSizeError, combiner.combine)

    def test_print(self):
        """
        method to test the print() method of csvCombiner
        """

        # initialize
        combiner = CC(files=["test_data/accessories.csv", "test_data/clothing.csv", "test_data/household_cleaners.csv"], chunk_size=10000)

        # incorrect data type stored
        combiner.combined = 5
        self.assertRaises(NoValueError, combiner.print)

        # correct output
        self.assertFalse(combiner.testing_flag)
        combiner.combine()
        combiner.print()
        self.assertTrue(combiner.testing_flag)
