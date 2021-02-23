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


### grab first file contents for header
def find_first_col_for_header():
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
def find_second_col_for_data():
    path = get_path()
    my_files = find_files(path)
    regex = re.compile('.*\t(.*)')

    data_headers = find_first_col_for_header()
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


###create dataframe header
# data_headers = find_first_col_for_header()
# print(data_headers)
# df = pd.DataFrame(data_headers,columns=['Item']).set_index('Item').T
# df = pd.DataFrame(columns=data_headers)
# df.drop(df.columns[[1]], axis=1, index=True)

# print(df)
# df.to_csv("df_headertest.csv")
# table = etl.fromdataframe(df)
# print(table)
# table.tocsv("etl_headertest.csv")
# data = find_second_col_for_data()
# print(data)
# data_csv = data.to_csv("test.csv")

data = find_second_col_for_data()

table = etl.fromdataframe(data)
print(table)

###write it to
etl.tocsv(table, "icp_test.csv")


# This is for merging dataframes together
def merge_csv(files_to_merge):
    dfs = [pd.read_csv(f) for f in files_to_merge]
    finaldf = pd.concat(dfs, axis=1, join='outer').to_csv("master.csv")
    return finaldf

# inputs = ["icp_it_works.csv", "procurement.csv","hot_press.csv"]
# merge_csv(inputs)
