# 02/25/2021 Matthew Kinzler for Citrine Informatics
import re
import os
import sys
import glob
import pandas as pd
import petl as etl
from pandas import DataFrame
from merger import Merger
from xlabdataengine import XLabDataEngine






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
refactored Merger.merge_csv() to fix a bug on line 26. Variable file_to_remove should 
return a filename, not a boolean. This section of the method .merge_csv() should generate a list of files
to merge, and if one of the files in the list is the master CSV, remove it from the list. I put this in
as an extra check to prevent the code from merging a master CSV file with other CSV files in the source path.
Line 26 should return the file name, so that we can iterate over the list of files to merge, and remove that filename 
from the list if the filename exists in the list. Prior to refactor, line 26 was checking if the 
file exists (True or False), then attempting to use that boolean value in the check for files
to merge - (i.e. find True or False in the list files_to_merge) - which is nonsense in this context.

Note that this bug had no impact on the code, because the method WarningGenerator.warn_if_master_in_source_path() 
forces the program to quit if a master CSV is found in the source path, and WarningGenerator.warn_if_master_in_source_path() 
is called before Merger.merge_csv() in the runner. Thus the program will quit before Merger.merge_csv() is executed 
if the master CSV file is found in the source path.
 '''