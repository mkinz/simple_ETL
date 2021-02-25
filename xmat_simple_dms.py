import re
import os
import sys
import glob
import pandas as pd
import petl as etl

class CSVMerger:

    """Merger class with single class method
    for merging all the CSV files together.
    It has built in protection to ensure that we
    do not join a master.csv file into the final
    master.csv file"""

    def merge_csv(self, source: str, destination: str) -> pd.DataFrame:
        # generate list of files to merge
        files_to_merge = glob.glob(os.path.join(source, "*csv"))

        # these next steps ensure we aren't merging a master.csv file with other files to make a new master.csv
        file_to_remove = os.path.isfile(os.path.join(source,"X-Materials_master_data.csv"))
        if file_to_remove in files_to_merge:
            files_to_merge.remove(file_to_remove)

        # create dataframe of the files in files_to_merge
        dfs = [pd.read_csv(file) for file in files_to_merge]

        # join the dataframes together with an outer join, then save it as the master.csv file
        finaldf = pd.concat(dfs, axis=1, join='outer').to_csv(os.path.join(destination, "X-Materials_master_data.csv"))
        return finaldf


class XLabDataEngine:
    """This class contains the methods
    that make up the engine which drives the conversion of
    the xlab data files into a formatted, single CSV file.
    The data is formatted in a 2x10 or 2x12 matrix,
    and the engine converts these files into a single
    10xN or 12xN matrix with all data contain within."""

    def set_up_headers(self, source: str, wildcard: str) -> list:
        """To convert the set of Hall*txt and ICP*txt files
        located in the path into a single 10xN or 12xN dataframe,
        we need do a lot of swizzling. First, we need to establish
        a header row, with all the data from column 1 of either file
        written out into a single row."""

        # set the path
        # path = source + wildcard
        path = os.path.join(source, wildcard)

        # list to hold files
        files_to_read = []

        # iterate over files in path, adding them to the list
        for file in glob.glob(path):
            files_to_read.append(file)

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
                header_row_content.append((col1))

        # return this list of header row content, it will be used later to create the full dataframe
        return header_row_content

    def build_xlab_dataframe(self, source: str, wildcard: str) -> pd.DataFrame:
        # functionally very similar and code not very DRY which hurts my soul a bit, but again, for time's sake
        # get all the files in the path -> this really should be it's own class method since it's exactly
        # the same as above
        # path = source + wildcard
        path = os.path.join(source, wildcard)
        files_to_read = []
        for file in glob.glob(path):
            files_to_read.append(file)

        # regex to capture the data from column 2 of the files
        regex = re.compile('.*\t(.*)')

        # instantiate an instance of the data engine, call the set_up_headers method to return list of headers
        data_headers = XLabDataEngine().set_up_headers(source, wildcard)

        # now we create a data frame using the instance created above
        df = pd.DataFrame(columns=data_headers)

        # this all repeats as above, again not DRY but we're going fast for time's sake
        for file in files_to_read:
            list_of_data_from_column_two_for_dataframe_row = []

            # open each file and read the contents
            with open(file, 'r') as f:
                mydata = f.read()

                # use regex findall method to find matches in the data
                for col2 in re.findall(regex, mydata):

                    # append list with matches
                    list_of_data_from_column_two_for_dataframe_row.append((col2))

                # take dataframe with headers only, and insert each line from
                # list_of_data_from_column_two_for_dataframe_row into the dataframe
                df.loc[file] = list_of_data_from_column_two_for_dataframe_row

        # return shiny new dataframe with all the data
        return df

    def xlab_csv_file_builder(self, source: str, destination: str) -> None:
        """this class method  instantiates the XlabDataEngine,
        calls the build methods, converts them to tables using the
        petl python library, and then writes the tables to csv files.
        Filenames are hard-coded in. """

        # build new XLabData dataframes with specific wildcards
        hall_data = XLabDataEngine().build_xlab_dataframe(source, "*Hall*txt")
        icp_data = XLabDataEngine().build_xlab_dataframe(source, "*ICP*txt")

        # convert dataframes to tables with petl, needed for correct data formatting
        hall_table = etl.fromdataframe(hall_data)
        icp_table = etl.fromdataframe(icp_data)

        # write tables to csv
        etl.tocsv(hall_table, os.path.join(destination, "hall_xlab.csv"))
        etl.tocsv(icp_table, os.path.join(destination, "icp_xlab.csv"))
        return


class Deleter:
    """Deleter class has two methods for deleting files.
    Currently unused but we have the option if needed"""

    def delete_temp_xlab_csv_files(self, source: str) -> None: #currently not using this methood
        xlab_files = ["hall_xlab.csv", "icp_xlab.csv"]
        for file in xlab_files:
            if os.path.isfile(os.path.join(source, file)):
                os.remove(os.path.join(source, file))

    def delete_current_master_csv_file(self, source: str) -> None:
        """look for a master csv file in the destination path
        and remove it if it exists"""

        master_csv = "X-Materials_master_data.csv"
        if os.path.isfile(os.path.join(source, master_csv)):
            os.remove(os.path.join(source, master_csv))
        return

class Warnings:
    """Warning class to give useful warnings when needed"""

    # attribute available for any instance to quickly throw a warning
    warning = 'WARNING'

    def warning_if_master_in_source_path(self, source: str) -> None:
        """look for a master csv file in the destination path
        and remove it if it exists"""

        master_csv = "X-Materials_master_data.csv"
        if os.path.isfile(os.path.join(source, master_csv)):
            print("It looks like you already have a "
                  "master CSV file in your source directory.\n"
                  "This will cause problems. Please remove it and try again.")
            sys.exit(0)
        return

    def warning_if_master_in_destination_path(self, destination: str) -> None:
        """look for a master csv file in the destination path
        and remove it if it exists"""

        master_csv = "X-Materials_master_data.csv"
        if os.path.isfile(os.path.join(destination, master_csv)):
            print("Master CSV file already exists in destination path.\n"
                  "OK to overwrite?\n"
                  "You can select [y]es or [n]o")
            affirmative = ["yes", "Yes", 'YES', 'y', 'Y']
            answer = input()
            if answer in affirmative:
                pass
            else:
                print("Exiting without doing anything.")
                sys.exit(0)
        return

class Runner:
    """This is the runner class for the command line interface
    that makes use of a single class method. Self explanatory,
    with conditional logic and a few loops to get input.
    Messy and should probably be broken into functions"""

    def cmd_line_interface(self) -> None:

        #instantiate classes
        myXlab_data_writer = XLabDataEngine()
        myWarningObj = Warnings()
        myMerger = CSVMerger()

        print("Welcome to the Data Management System application!")
        print("Please enter the source path now:\n")
        while True:
            source = input()
            if not os.path.isdir(source):
                print(f"{source} is not a valid path.\n\nPlease double check your path "
                      f"and try running the app again.")
                sys.exit()
            print("Please enter the destination (path to save files) now:\n")
            destination = input()
            if not os.path.isdir(destination):
                print(f"{destination} is not a valid path.\n\nPlease double check your path "
                      f"and try running the app again.")
                sys.exit()
            print(f"Your source path is: \n{source}\n\n and destination path is: \n{destination}\n")
            if source == destination:
                print(myWarningObj.warning)
                print("Source and destination are the same path!\n")
            print("Is this correct? Type [y]es or [n]o.")
            answer = input()
            affirmative = ["yes", "Yes", 'YES', 'y', 'Y']
            negative = ["no", "No", "NO", "n", "N", "no siree"]

            if answer in affirmative:
                break
            elif answer in negative:
                print("Ok, exiting. Please check your paths and try again.")
                sys.exit() #this should probably be in a while loop
        print("\nSource and destination paths saved.\n")
        while True:
            print('What would you like to do?')
            print("1. Generate new master CSV from existing files in source")
            print("2: Quit")

            # the program only does one thing now, but I built the option selector for easy extension future extensibilly
            try:
                options = int(input())
                if options == 1:
                    try:
                        print("Generating master.csv file now...\n")
                        print(myWarningObj.warning)
                        # generate X-lab csv files
                        myXlab_data_writer.xlab_csv_file_builder(source, destination)

                        # throw warnings if necessary
                        myWarningObj.warning_if_master_in_source_path(source)
                        myWarningObj.warning_if_master_in_destination_path(destination)

                        # run the merger, merge *csv files in source, write output master csv to destination
                        myMerger.merge_csv(source, destination)

                        print(f"Done.\nX-Materials_master_CSV.csv file "
                              f"located in: \n{destination}\nExiting. Have a nice day!")
                        break
                    except IndexError:
                        print(myWarningObj.warning)
                        print(f"Cannot continue. Possibly missing data in your source path."
                              f"\nPlease double check your source path. It"
                              f" is currently set to: \n\n{source}\n")
                        sys.exit(-1)
                    except PermissionError:
                        print(myWarningObj.warning)
                        print(f"Cannot continue.\n"
                              f"Please close all CSV and TXT files in source path, and try again.")
                        sys.exit(0)
                elif options == 2:
                    print("Exiting without doing anything.\nHave a nice day!")
                    sys.exit(0)
                else:
                    print("Sorry, I didn't understand that.\nYou can only select 1 or 2.")
            except ValueError:
                print("You can only select 1 or 2.")
            else:
                print("I didn't understand that. Please try again.")

            return

def main():

    # instantiate a runner
    run_the_code = Runner()

    # then run it!
    run_the_code.cmd_line_interface()

if __name__ == '__main__':
    main()



#source = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'
#destination = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'

'''source = 'C:\\Users\\matth\\Downloads\\dae-challenge'
destination = 'C:\\Users\\matth\\Downloads\\dae-challenge'
myXlab_data_writer = XLabDataEngine()
myXlab_data_writer.xlab_csv_file_builder(source, destination)'''