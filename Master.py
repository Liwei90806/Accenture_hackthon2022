# the following program and included projects ahve been provided to you for the use with the Accenture Hackathon event only.
# the below code should not, and will not be used for any other application and must be deleted following the UWAYE Hackathon Event.
from ctypes.wintypes import INT
import Mod
import datetime
from time import sleep
from bitstring import BitArray
import pandas as pd
import argparse

#The following function logs the Modbus Data from Mod.py into a CSV file within the application folder.
def master(historicalDataFrame, runFlag, previousMinute):
       
	while runFlag == True:

		rtn, day, previousMinuteReturn = Mod.ExecuteProcess(historicalDataFrame, previousMinute)
		print(historicalDataFrame)
		print(previousMinute)
		previousMinute = previousMinuteReturn
		print('Day')
		print(day)
		sleep(0.5)
	
	# Export Datafram to CSV once simulation is complete.
		historicalDataFrame.to_csv("historicalDataFrame.csv")


		
if __name__ == "__main__":
	# Defining Dataframe Columns
	columns = ['Day', 'Hour', 'Minutes', 'Weather', 'Temperature (C)','Supply Pump 1 Run Sts', 'Supply Pump 1 Speed (%)','Supply Pump 1 Flow Rate (l/s)', 'Supply Pump 1 Power (KWh)', 'Supply Pump 2 Run Sts', 'Supply Pump 2 Speed (%)', 'Supply Pump 2 Flow Rate (l/s)', 'Supply Pump 2 Power (KWh)', 'Discharge Pump Run Sts', 'Discharge Pump Speed (%)', 'Discharge Pump Flow Rate (l/s)', 'Discharge Pump Power (KWh)', 'Tank Level (Kl)', 'Site Power Load (KW)', 'Daily Site Energy Usage (KWh)', ' Solar Generation (KW)', 'Daily Solar Energy Usage (KWh)', 'Daily Grid Energy Usage (KWh)']
	# Initalizing DataFrame
	historicalDataFrame = pd.DataFrame(columns=columns)
	runFlag = True
	previousMinute = 0
	master(historicalDataFrame, runFlag, previousMinute)