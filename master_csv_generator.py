import re
import pandas as pd
import petl as etl
import os
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

source = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'
destination = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'

#write_em = XLabData().write_new_xlab_csv_files(source, destination)

# This is for merging dataframes together
def merge_csv(source, destination):
    files_to_merge = glob.glob(source + "\\*csv")
    dfs = [pd.read_csv(f) for f in files_to_merge]
    finaldf = pd.concat(dfs, axis=1, join='outer').to_csv(destination + "\\master.csv")
    return finaldf

merge_csv(source, destination)

