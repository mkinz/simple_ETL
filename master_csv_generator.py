import re
import pandas as pd
import petl as etl
import os
import glob


def get_path():
    pwd = 'C:\\Users\\matth\\Downloads\\dae-challenge\\dae-challenge\\x-lab-data'
    wildcard = "\\*ICP*txt"
    path = pwd + wildcard
    return path


def find_files(path):
    files_to_read = []
    for file in glob.glob(path):
        files_to_read.append(file)
    return files_to_read


class XLabData:
    def set_up_headers(self):
        path = get_path()
        my_files = find_files(path)
        regex = re.compile('(.*)\t.*')

        found_stuff = []
        # use first file in files_to_read to get headers
        with open(my_files[0], 'r') as f:
            mydata = f.read()
            for col1 in re.findall(regex, mydata):
                found_stuff.append((col1))
        return found_stuff


    ### iterate over all files to get data from second column
    def build_xlab_dataframe(self):
        path = get_path()
        my_files = find_files(path)
        regex = re.compile('.*\t(.*)')

        data_headers = XLabData().set_up_headers()
        df = pd.DataFrame(columns=data_headers)
        # print(df)

        for file in my_files:
            list_of_found_stuff = []
            with open(file, 'r') as f:
                mydata = f.read()
                for col2 in re.findall(regex, mydata):
                    list_of_found_stuff.append((col2))
                df.loc[file] = list_of_found_stuff
        return df



data = XLabData().build_xlab_dataframe()

table = etl.fromdataframe(data)
print(table)

###write it to
#etl.tocsv(table, "icp_test.csv")


# This is for merging dataframes together
def merge_csv(files_to_merge):
    dfs = [pd.read_csv(f) for f in files_to_merge]
    finaldf = pd.concat(dfs, axis=1, join='outer').to_csv("master.csv")
    return finaldf

# inputs = ["icp_it_works.csv", "procurement.csv","hot_press.csv"]
# merge_csv(inputs)
