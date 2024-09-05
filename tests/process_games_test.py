import sys, os
os.chdir("..")
sys.path.append(os.getcwd())

from hockeylogic.ProcessGameEvents import ProcessGameEvents
from interfaces.IMYSQLService import IMYSQLService
from datetime import datetime 

mock = IMYSQLService()

processor = ProcessGameEvents(sql=mock)
for day in [datetime(2024, 3, 4), datetime(2024, 3, 5)]:
    processor.ProcessSeason(day)
print("done")
mock.Close()