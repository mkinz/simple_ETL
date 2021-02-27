# 02/25/2021 Matthew Kinzler for Citrine Informatics
import re
import os
import sys
import glob
import pandas as pd
import petl as etl
from pandas import DataFrame


class Merger:
    """Merger class with single class method
    for merging all the CSV files together.
    It has built in protection to ensure that we
    do not join a master.csv file into the final
    master.csv file"""

    # none of these methods change the state of the class, so they are static
    @staticmethod
    def merge_csv(source: str, destination: str) -> pd.DataFrame:

        # generate list of files to merge
        files_to_merge = glob.glob(os.path.join(source, "*csv"))

        # these next steps ensure we aren't merging a master.csv file with other files to make a new master.csv
        # Note: this is redundant, as the WarningGenerator.warn_if_master_in_source_path() will exit the program
        # if a master CSV is found in the source path
        file_to_remove = os.path.join(source, "X-Materials_master_data.csv")

        # if the master CSV file is in the list files_to_merge, remove it
        if file_to_remove in files_to_merge:
            files_to_merge.remove(file_to_remove)

        # get a list of the file names that will be merged
        file_names = [os.path.basename(source) for source in files_to_merge]

        # show the names of files to merge, and confirm
        print("The following files will be merged:\n")

        for name in file_names:
            print(name)

        print("\nConfirm merge? Type [y]es or [n]o.")

        affirmative = ["yes", "Yes", 'YES', 'y', 'Y']  # definitely should be a regex match

        answer = input()

        if answer in affirmative:

            # create list of dataframe objects from files in files_to_merge
            dfs = [pd.read_csv(file) for file in files_to_merge]

            # join the dataframes together with an outer join, then save it as the master.csv file
            merged_dataframe = pd.concat(dfs, axis=1, join='outer') \
                .to_csv(os.path.join(destination, "X-Materials_master_data.csv"))

            return merged_dataframe
        else:
            print("Exiting without doing anything.")
            sys.exit(0)


class XLabDataEngine:
    """This class contains the methods that drives the conversion of
    the xlab data files into a formatted, single CSV file.
    The raw data is formatted in a 2x10 or 2x12 matrix,
    and the engine converts these files into a single
    10xN or 12xN matrix csv file with all data contain within."""

    @staticmethod
    def set_up_headers(source: str, wildcard: str) -> list:
        """To convert the set of Hall*txt and ICP*txt files
        located in the path into a single 10xN or 12xN dataframe,
        we need do a lot of swizzling. First, we need to establish
        a header row, with all the data from column 1 of either file
        written out into a single row."""

        # set the path
        path = os.path.join(source, wildcard)

        # iterate over files in path, adding them to a list
        files_to_read = [file for file in glob.glob(path)]

        # regex to capture all the data in column 1 of Hall*txt of ICP*txt
        regex = re.compile('(.*)\t.*')

        # another list to hold the data found with the regex capture
        header_row_content = []

        # use first file in files_to_read to get headers
        # (this is definitely memory inefficient, but it's simple and works for the sake of time)
        with open(files_to_read[0], 'r') as f:
            # read the contents of the first file in files_to_read
            mydata = f.read()

            # iterate on the data in column 1 using regex findall method
            for col1 in re.findall(regex, mydata):
                # for each item found, add it to header_row_content
                header_row_content.append(col1)

        # return this list of header row content, it will be used later to create the full dataframe
        return header_row_content

    @staticmethod
    def build_xlab_dataframe(source: str, wildcard: str) -> pd.DataFrame:

        # functionally very similar as above and not very DRY which hurts my soul a bit
        path = os.path.join(source, wildcard)
        files_to_read = []
        for file in glob.glob(path):
            files_to_read.append(file)

        # regex to capture the data from column 2 of the files
        regex = re.compile('.*\t(.*)')

        # instantiate an instance of the data engine, call the set_up_headers method to return list of headers
        data_headers = XLabDataEngine().set_up_headers(source, wildcard)

        # now we create a data frame using the instance created above
        df: DataFrame = pd.DataFrame(columns=data_headers)

        # this all repeats as above, again not DRY but we're going fast for time's sake
        for file in files_to_read:
            list_of_data_from_column_two_for_dataframe_row = []

            # open each file and read the contents
            with open(file, 'r') as f:
                mydata = f.read()

                # use regex findall method to find matches in the data
                for col2 in re.findall(regex, mydata):
                    # append list with matches
                    list_of_data_from_column_two_for_dataframe_row.append(col2)

                # take dataframe with headers only, and insert each line from
                # list_of_data_from_column_two_for_dataframe_row into the dataframe
                df.loc[file] = list_of_data_from_column_two_for_dataframe_row

        # return shiny new dataframe with all the data
        return df

    @staticmethod
    def build_xlab_csv_files(source: str, destination: str) -> None:
        """this class method  instantiates the XlabDataEngine,
        calls the build methods, converts them to tables using the
        petl python library, and then writes the tables to csv files.
        Filenames are hard-coded in. Not DRY, should be refactored. """

        # build hall and icp dataframes
        hall_data = XLabDataEngine().build_xlab_dataframe(source, "*Hall*txt")
        icp_data = XLabDataEngine().build_xlab_dataframe(source, "*ICP*txt")

        # convert dataframes to tables with petl, needed for correct data formatting
        hall_table = etl.fromdataframe(hall_data)
        icp_table = etl.fromdataframe(icp_data)

        # write tables to csv
        etl.tocsv(hall_table, os.path.join(source, "hall_xlab.csv"))
        etl.tocsv(icp_table, os.path.join(source, "icp_xlab.csv"))

        return


class Deleter:
    """Deleter class has two methods for deleting files.
    Currently unused but we have the option if needed"""

    # currently not using this method
    @staticmethod
    def delete_temp_xlab_csv_files(source: str) -> None:

        xlab_files = ["hall_xlab.csv", "icp_xlab.csv"]
        for file in xlab_files:
            if os.path.isfile(os.path.join(source, file)):
                os.remove(os.path.join(source, file))
        return

    # currently not using this method
    @staticmethod
    def delete_current_master_csv_file(source: str) -> None:

        # define filename
        master_csv = "X-Materials_master_data.csv"

        # if the file exists, remove it
        if os.path.isfile(os.path.join(source, master_csv)):
            os.remove(os.path.join(source, master_csv))
        return


class WarningGenerator:
    """Warning class to give useful warnings when needed"""

    # static variable
    warning = "WARNING"

    @staticmethod
    def warn_if_master_in_source_path(source: str) -> None:
        """look for a master csv file in the destination path
        and remove it if it exists"""

        # define filename
        master_csv = "X-Materials_master_data.csv"

        # if found, print a warning message
        if os.path.isfile(os.path.join(source, master_csv)):
            print("It looks like you already have a "
                  "master CSV file in your source directory.\n"
                  "This will cause problems. Please remove it and try again.")
            sys.exit(0)
        return

    @staticmethod
    def warn_if_master_in_destination_path(destination: str) -> None:
        """look for a master csv file in the destination path
        and remove it if it exists"""

        # define filename
        master_csv = "X-Materials_master_data.csv"
        if os.path.isfile(os.path.join(destination, master_csv)):

            # if found, print a warning message, ask if overwrite is ok
            print("Master CSV file already exists in destination path.\n"
                  "OK to overwrite?\n"
                  "You can select [y]es or [n]o")

            affirmative = ["yes", "Yes", 'YES', 'y', 'Y']

            answer = input()

            # if overwrite is ok, then continue back to main runner
            if answer in affirmative:
                pass

            # otherwise, get the heck out of here
            else:
                print("Exiting without doing anything.")
                sys.exit(0)
        return


class Runner:
    """This is the runner class for the command line interface
    that makes use of a single class method. Self explanatory,
    with conditional logic and a few loops to get input.
    Messy and should probably be broken into functions"""

    @staticmethod
    def cmd_line_interface() -> None:

        print("Welcome to the Simple Data Management System!")

        print("Please enter the source path now:\n")
        source = input()

        # input validation
        if not os.path.isdir(source):
            print(f"{source} is not a valid path.\n\nPlease double check your path "
                  f"and try running the app again.")
            sys.exit()

        print("Please enter the destination (path to save files) now:\n")
        destination = input()

        # input validation
        if not os.path.isdir(destination):
            print(f"{destination} is not a valid path.\n\nPlease double check your path "
                  f"and try running the app again.")
            sys.exit()

        # print source and destination to stdout
        print(f"Your source path is: \n{source}\n\n and destination path is: \n{destination}\n")

        # warning if source and destination match
        if source == destination:
            print(WarningGenerator.warning)
            print("Source and destination are the same path!\n")
        print("Is this correct? Type [y]es or [n]o.")

        answer = input()
        affirmative = ["yes", "Yes", 'YES', 'y', 'Y']  # definitely should use regex match here

        # conditional do stuff
        if answer in affirmative:
            try:
                print("Generating master.csv file now...\n")
                # generate X-lab csv files
                XLabDataEngine.build_xlab_csv_files(source, destination)

                # throw warnings if necessary
                WarningGenerator.warn_if_master_in_source_path(source)
                WarningGenerator.warn_if_master_in_destination_path(destination)

                # run the merger, merge *csv files in source, write output master csv to destination
                Merger.merge_csv(source, destination)

                print(f"Done.\nX-Materials_master_data.csv file "
                      f"written to: \n{destination}\nExiting. Have a nice day!")

            # catch both index and value errors together in this tuple since the warning is the same
            except (IndexError, ValueError):
                print(WarningGenerator.warning)
                print(f"Cannot continue. Possibly missing data in your source path."
                      f"\nPlease double check your source path. It"
                      f" is currently set to: \n\n{source}\n")
                sys.exit(-1)

            # warning if trying to work write files already open
            except PermissionError:
                print(WarningGenerator.warning)
                print(f"Cannot continue.\n"
                      f"Please close all CSV and TXT files in source path, and try again.")
                sys.exit(0)
        else:
            print(WarningGenerator.warning)
            print("Need to confirm that it's OK for source and destination path to match.")
            print("Exiting for safety!")
            sys.exit()

        return


def main():
    # run it!
    Runner.cmd_line_interface()


if __name__ == '__main__':
    main()

'''
########################
###### CHANGE LOG ######
########################

02/27/2021 
refactored Merger.merge_csv() method to fix line 26. variable file_to_remove should 
return a filename, not a boolean. This section of the method .merge_csv() should generate a list of files
to merge, and if one of the files in the list is the master CSV, remove it from the list. I put this in
as an extra check to prevent the code from merging a master CSV file in the source path with other CSV files.
Prior to refactor, the code was checking if the file exists (True or False) rather than returning the file name.
Note that this bug had no impact on the code, because of the WarningGenerator.warn_if_master_in_source_path() 
class method which exits the program if a master CSV is found in the source path.
 '''