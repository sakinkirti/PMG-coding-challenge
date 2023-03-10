import pandas as pd
import os
import sys

class csvCombiner:
    """
    class which takes csv files as input and combines them to one file
    """

    def __init__(self, files: list, chunk_size: int) -> None:
        """
        generate variables to store data
        
        params:
        :files: list, the list of filepaths containing the csv files
        """

        self.files = files
        self.combined = pd.DataFrame()
        self.chunk_size = chunk_size
        self.testing_flag = False

    def combine(self):
        """
        method which combines csv files
        """

        # check the number of inputs
        if len(self.files) < 1:
            raise NoFilesError("No files given to parse.")
        elif len(self.files) >= 1:
            # check that given files exist and that they have data
            for file in self.files:
                if not os.path.exists(file):
                    raise FileNotFoundError(f"Could not locate the file: {file}")
                if os.stat(file).st_size == 0:
                    raise FileSizeError(f"The given file contains no data: {file}")

        # combine the csv files
        combined_df = pd.DataFrame()

        # parse the files
        for file in self.files:
            # read using chunks to mitigate memory issues
            for temp in pd.read_csv(file, chunksize=self.chunk_size):
                filename = os.path.basename(file)

                # add filename column
                temp["filename"] = filename
                combined_df = pd.concat([combined_df, temp], ignore_index=True)

        # store the df
        self.combined = combined_df

    def print(self):
        """
        method to print the csv to the stdout
        """

        if isinstance(self.combined, pd.DataFrame):
            sys.stdout.write(self.combined.to_csv())
            self.testing_flag = True
        else:
            raise NoValueError("The incorrect data type is stored. Cannot print.")

class NoFilesError(Exception): pass
class FileNotFoundError(Exception): pass
class FileSizeError(Exception): pass
class NoValueError(Exception): pass

def main():
    """
    main method - when called, takes arguments from the command line in the format
    <script name> <csv 1> <csv 2> <csv 3> ... > <output csv>

    main method takes all input csv files and generates a single output csv with all
    data present
    """

    # initialize the combiner and combine csv files
    combiner = csvCombiner(files=sys.argv[1:], chunk_size=10000)
    combiner.combine()

    # print to stdout
    combiner.print()

if __name__ == "__main__":
    main()