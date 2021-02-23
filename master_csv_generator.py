import re
import pandas as pd
import petl as etl
import os
import sys
import glob

class XLabData:

    def set_up_headers(self,source, wildcard):
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
    def build_xlab_dataframe(self, source, wildcard):
        path = source + wildcard
        files_to_read = []

        for file in glob.glob(path):
            files_to_read.append(file)
        regex = re.compile('.*\t(.*)')

        data_headers = XLabData().set_up_headers(source, wildcard)
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


    def write_new_xlab_csv_files(self, source, destination):
        #build new XLabData dataframes with specific wildcards
        hall_data = XLabData().build_xlab_dataframe(source, "\\*Hall*txt")
        icp_data = XLabData().build_xlab_dataframe(source, "\\*ICP*txt")
        #convert dataframes to tables with petl
        hall_table = etl.fromdataframe(hall_data)
        icp_table = etl.fromdataframe(icp_data)

        print(hall_table)
        print(icp_table)
        #write tables to csv
        etl.tocsv(hall_table, destination + "\\hall_xlab.csv")
        etl.tocsv(icp_table,  destination + "\\icp_xlab.csv")
        return


class CSVMerger:
    # This is for merging dataframes together
    def merge_csv(self,source, destination):
        files_to_merge = glob.glob(source + "\\*csv")
        dfs = [pd.read_csv(f) for f in files_to_merge]
        finaldf = pd.concat(dfs, axis=1, join='outer').to_csv(destination + "\\master.csv")
        return finaldf


'''source = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'
destination = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'

xlab_data_writer = XLabData().write_new_xlab_csv_files(source, destination)
merger = CSVMerger().merge_csv(source, destination)'''


class Runner:
    def cmd_line_interface(self):
        print("Welcome to the Data Management System application!")
        while True:
            print("Please enter the source path now:\n")
            source = input()
            print("Please enter the destination (path to save files) now:\n")
            destination = input()
            print(f"Your source path is: \n{source}\n\n and destination path is: \n{destination}\n")
            print("Is this correct? Type [y]es or [n]o.")
            answer = input()
            answers = ["yes", "Yes", 'YES', 'y', 'Y']
            if answer in answers:
                print('What would you like to do?')
                print("1. Generate new master CSV from existing files in source")
                print("2: Load new Xlab data and generate a new master CSV")
                print("3: Quit")
                options = int(input())
                if options == 1:
                    myMerger = CSVMerger()
                    myMerger.merge_csv(source, destination)
                    break
                elif options == 2:
                    myXlab_data_writer = XLabData()
                    myXlab_data_writer.write_new_xlab_csv_files(source, destination)
                    myMerger = CSVMerger()
                    myMerger.merge_csv(source, destination)
                    break
                elif options == 3:
                    sys.exit(0)
                else:
                    pass

        sys.exit(0)

myrunner = Runner().cmd_line_interface()