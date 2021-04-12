import os
import re
import glob
import pandas as pd
import petl as etl

class LabDataFormatter:
    """This class contains the methods that drives the conversion of
    the xlab data files into a formatted, single CSV file.
    The raw data is formatted in a 2x10 or 2x12 matrix,
    and the engine converts these files into a single
    10xN or 12xN matrix csv file with all data contain within."""

    def set_up_headers(self, source: str, wildcard: str) -> list:
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

    def build_labdata_dataframe(self, source: str, wildcard: str) -> pd.DataFrame:

        # functionally very similar as above and not very DRY which hurts my soul a bit
        path = os.path.join(source, wildcard)
        files_to_read = []
        for file in glob.glob(path):
            files_to_read.append(file)

        # regex to capture the data from column 2 of the files
        regex = re.compile('.*\t(.*)')

        # instantiate an instance of the data engine, call the set_up_headers method to return list of headers
        data_headers = LabDataFormatter().set_up_headers(source, wildcard)

        # now we create a data frame using the instance created above
        df= pd.DataFrame(columns=data_headers)

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

    def build_labdata_csv_files(self, source: str, destination: str) -> None:
        """this method  instantiates the XlabDataEngine,
        calls the build methods, converts them to tables using the
        petl python library, and then writes the tables to csv files.
        Filenames are hard-coded in. Not DRY, should be refactored. """

        # build hall and icp dataframes
        hall_data = LabDataFormatter().build_labdata_dataframe(source, "*Hall*txt")
        icp_data = LabDataFormatter().build_labdata_dataframe(source, "*ICP*txt")

        # convert dataframes to tables with petl, needed for correct data formatting
        hall_table = etl.fromdataframe(hall_data)
        icp_table = etl.fromdataframe(icp_data)

        # write tables to csv
        etl.tocsv(hall_table, os.path.join(source, "lab1lab.csv"))
        etl.tocsv(icp_table, os.path.join(source, "lab2lab.csv"))

        return

