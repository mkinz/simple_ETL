import re
import pandas as pd
import petl as etl
import os
import sys
import glob

class CSVMerger:
    """Merger class with single class method
    for merging all the CSV files together."""

    def merge_csv(self,source: str, destination: str) -> pd.DataFrame:
        files_to_merge = glob.glob(source + "\\*csv")
        dfs = [pd.read_csv(f) for f in files_to_merge]
        finaldf = pd.concat(dfs, axis=1, join='outer').to_csv(destination + "\\master.csv")
        return finaldf


class XLabDataEngine:
    """This class contains the methods
    that make up the engine which drives the conversion of
    the xlab data files into a formatted, single CSV file.
    The data is formatted in a 2x10 or 2x12 matrix,
    and the engine converts these files into a single
    10xN or 12xN matrix with all data contain within."""

    def set_up_headers(self,source: str, wildcard: str) -> list:
        """To convert the set of Hall*txt and ICP*txt files
        located in the path into a single 10xN or 12xN dataframe,
        we need do a lot of swizzling. First, we need to establish
        a header row, with all the data from column 1 of either file
        written out into a single row."""

        # set the path
        path = source + wildcard
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
        path = source + wildcard
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


    def write_new_xlab_csv_files(self, source: str, destination: str) -> None:
        #build new XLabData dataframes with specific wildcards
        hall_data = XLabDataEngine().build_xlab_dataframe(source, "\\*Hall*txt")
        icp_data = XLabDataEngine().build_xlab_dataframe(source, "\\*ICP*txt")
        #convert dataframes to tables with petl
        hall_table = etl.fromdataframe(hall_data)
        icp_table = etl.fromdataframe(icp_data)

        print(hall_table)
        print(icp_table)
        #write tables to csv
        etl.tocsv(hall_table, os.path.join(destination, "hall_xlab.csv"))
        etl.tocsv(icp_table,  os.path.join(destination, "icp_xlab.csv"))
        return




class Deleter:

    """Deleter class has two methods for deleting files.
    Currently only using the delete_current_master_csv_file()
    class method in the runner to protect against possibly
    including the master.csv in the collection of individual
    CSV files."""

    def delete_temp_xlab_csv_files(self, destination: str) -> None:
        path = destination
        xlab_files = ["hall_xlab.csv","icp_xlab.csv"]
        for file in xlab_files:
            if os.path.isfile(os.path.join(path, file)):
                os.remove(os.path.join(path, file))

    def delete_current_master_csv_file(self, destination: str) -> None:
        path = destination
        master_csv = "master.csv"
        if os.path.isfile(os.path.join(path,master_csv)):
            os.remove(os.path.join(path,master_csv))
        return



class Runner:
    """This is the runner class for the command line interface
    that makes use of a single class method. Self explanatory,
    with conditional logic and a few loops to get input."""

    def cmd_line_interface(self) -> None:
        print("Welcome to the Data Management System application!")
        while True:
            print("Please enter the source path now:\n")
            source = input()
            if not os.path.isdir(source):
                print(f"{source} is not a valid path.\nPlease double check your path "
                      f"and try running the app again.")
                break
            print("Please enter the destination (path to save files) now:\n")
            destination = input()
            if not os.path.isdir(destination):
                print(f"{destination} is not a valid path.\nPlease double check your path "
                      f"and try running the app again.")
            print(f"Your source path is: \n{source}\n\n and destination path is: \n{destination}\n")
            if source == destination:
                print("WARNING: source and destination are the same path!\n")
            print("Is this correct? Type [y]es or [n]o.")
            answer = input()
            affirmative = ["yes", "Yes", 'YES', 'y', 'Y']
            negative = ["no", "No", "NO", "n", "N", "no siree"]
            if answer in affirmative:
                break
            elif answer in negative:
                print("OK, please try again.")
            else:
                print("I didn't understand that. Please try again.")
        print("source and destination paths saved.")
        while True:
            print('What would you like to do?')
            print("1. Generate new master CSV from existing files in source")
            print("2: Quit")
            try:
                options = int(input())
                if options == 1:
                    try:
                        print("Generating master.csv file now...\n")
                        myXlab_data_writer = XLabDataEngine()
                        myXlab_data_writer.write_new_xlab_csv_files(source, destination)
                        myDeleter = Deleter().delete_current_master_csv_file(destination)
                        myMerger = CSVMerger()
                        myMerger.merge_csv(source, destination)
                        print(f"Done.\nmaster.csv file located in: \n{destination}\nExiting. Have a nice day!")
                        break
                    except IndexError:
                        print(f"WARNING: Cannot continue. Please double check your source path. It"
                              f" is currently set to \n{source}\n")
                        sys.exit(-1)
                elif options == 2:
                    print("Exiting without doing anything.\nHave a nice day!")
                    sys.exit(0)
                else:
                    print("Sorry, I didn't understand that.\nYou can only select 1 or 2.")
            except ValueError:
                print("You can only select 1 or 2.")
        return

myrunner = Runner().cmd_line_interface()

'''source = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'
destination = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'
'''