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
        path = source + wildcard
        files_to_read = []
        for file in glob.glob(path):
            files_to_read.append(file)

        regex = re.compile('(.*)\t.*')

        found_stuff = []
        # use first file in files_to_read to get headers
        with open(files_to_read[0], 'r') as f:
            mydata = f.read()
            for col1 in re.findall(regex, mydata):
                found_stuff.append((col1))
        return found_stuff


    ### iterate over all files to get data from second column
    def build_xlab_dataframe(self, source: str, wildcard: str) -> pd.DataFrame:
        path = source + wildcard
        files_to_read = []

        for file in glob.glob(path):
            files_to_read.append(file)
        regex = re.compile('.*\t(.*)')

        data_headers = XLabDataEngine().set_up_headers(source, wildcard)
        df = pd.DataFrame(columns=data_headers)
        # print(df)

        for file in files_to_read:
            list_of_found_stuff = []
            with open(file, 'r') as f:
                mydata = f.read()
                for col2 in re.findall(regex, mydata):
                    list_of_found_stuff.append((col2))
                df.loc[file] = list_of_found_stuff
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
        etl.tocsv(hall_table, destination + "\\hall_xlab.csv")
        etl.tocsv(icp_table,  destination + "\\icp_xlab.csv")
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
                break
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