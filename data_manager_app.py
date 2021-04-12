from runner import Runner
from merger import Merger
from labdataformatter import LabDataFormatter
from warning_generator import WarningGenerator


def main():
    # run it!
    myRunner = Runner(Merger(), LabDataFormatter(), WarningGenerator())
    myRunner.cmd_line_interface()


if __name__ == '__main__':
    main()