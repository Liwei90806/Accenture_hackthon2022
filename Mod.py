
from base64 import decode
import os.path
import json
import logging
import itertools
from datetime import datetime, timedelta, date, time
from time import sleep
from bitstring import BitArray
#import pandas as pd

from pymodbus.client.sync import ModbusTcpClient as ModbusClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.payload import BinaryPayloadDecoder


#Execute
def ExecuteProcess(historicalDataframe, previousMinute):
    rtn = True

    #set current time
    ts = datetime.utcnow()    
   
    # establish modbus connection
    try:
        modbusconn = ModbusConnection()
        if not modbusconn.Connect():
            return False 
    except:
        return False
    

    #Read from the registers
    day, previousMinuteReturn = modbusconn.ReadWriteRegisters(historicalDataframe, previousMinute)

    #Close modbus connection
    modbusconn.Disconnect()

    return rtn, day, previousMinuteReturn




class ModbusConnection:

    """class to perform connectivity to modbus slave, read/write to registers with a given payload"""

    def __init__(self):
        self.RegisterStartAddr = 1000
        self.lastHeartbeatValue = 0
        self.client = ModbusClient(0, 0)
        self.LastConfidentTimeStamp = datetime.now()
        
    # The following function defines the PLC connection details to retrieve the real-time data
    def Connect(self):
        #establish connection to modbus device
        try:
            self.client = ModbusClient("10.10.100.100", port=502)
            if not self.client.connect():
                print("Connect: modbus not established")
                return False
            else:
                print("Connect: modbus established")
                return True
        except:
            print("Connect: Exception", exc_info=1)
            raise

    def Disconnect(self):
        #disconnect from modbus device
        self.client.close()

    # THe following section details the Reading and writing data registers. Actual writing to the PLC will ONLY be enabled during demonstration testing. 
    # If the Correct BOOLEANS turn on and off in the Write section, they should turn on and off in the PLC during Demonstartion.
       
    def ReadWriteRegisters(self, historicalDataframe, previousMinute):
        #reads from modbus registers
        try:
            # Reading Current Minutes Value
            readMinutes = self.client.read_holding_registers(
               1002,1,unit=0)
            decoderMinutes = BinaryPayloadDecoder.fromRegisters(readMinutes.registers, byteorder=Endian.Big, wordorder=Endian.Little)
            Minute = decoderMinutes.decode_16bit_int()
            
            # Checking to see when the next Data is avaliable
            while Minute == previousMinute:
                readMinutes = self.client.read_holding_registers(
                    1002,1,unit=0)
                decoderMinutes = BinaryPayloadDecoder.fromRegisters(readMinutes.registers, byteorder=Endian.Big, wordorder=Endian.Little)
                Minute = decoderMinutes.decode_16bit_int()

            # Reading all the registers Starting at %MW1000
            read = self.client.read_holding_registers(
               1000,37,unit=0)
            realVal = read.registers
            print(realVal)
            ###
            decoder = BinaryPayloadDecoder.fromRegisters(realVal, byteorder=Endian.Big, wordorder=Endian.Little)
            Day = decoder.decode_16bit_int() #%MW1000
            Hour = decoder.decode_16bit_int()
            Minute = decoder.decode_16bit_int()
            WeatherTypeIndex = decoder.decode_16bit_int()
            SupplyPump1Speed = decoder.decode_32bit_float()
            SupplyPump1FlowRate = decoder.decode_32bit_float()
            SupplyPump1Power = decoder.decode_32bit_float()
            SupplyPump2Speed = decoder.decode_32bit_float()
            SupplyPump2FlowRate = decoder.decode_32bit_float()
            SupplyPump2Power = decoder.decode_32bit_float()    
            DischargePumpSpeed = decoder.decode_32bit_float()
            DischargePumpFlowRate = decoder.decode_32bit_float()
            DischargePumpPower = decoder.decode_32bit_float()       
            TankLevel = decoder.decode_32bit_float()
            CurrentPowerUsage = decoder.decode_32bit_float()
            DailyPowerUsage = decoder.decode_32bit_float()
            CurrentSolarGeneration = decoder.decode_32bit_float()
            DailySolarGeneration = decoder.decode_32bit_float()
            CurrentTemperature = decoder.decode_32bit_float()
            DailyPowerFromTheGrid = decoder.decode_32bit_float()
            PumpsRunningStatus = decoder.decode_16bit_int()

            
            # Unboxing Pumps Running Statuses
            if PumpsRunningStatus == 0:
                SupplyPump1Running = False
                SupplyPump2Running = False
                DischargePumpRunning = False
            elif PumpsRunningStatus == 1:
                SupplyPump1Running = True
                SupplyPump2Running = False
                DischargePumpRunning = False
            elif PumpsRunningStatus == 2:
                SupplyPump1Running = False
                SupplyPump2Running = True
                DischargePumpRunning = False
            elif PumpsRunningStatus == 3:
                SupplyPump1Running = True
                SupplyPump2Running = True
                DischargePumpRunning = False
            elif PumpsRunningStatus == 4:
                SupplyPump1Running = False
                SupplyPump2Running = False
                DischargePumpRunning = True
            elif PumpsRunningStatus == 5:
                SupplyPump1Running = True
                SupplyPump2Running = False
                DischargePumpRunning = True
            elif PumpsRunningStatus == 6:
                SupplyPump1Running = False
                SupplyPump2Running = True
                DischargePumpRunning = True
            elif PumpsRunningStatus == 7:
                SupplyPump1Running = True
                SupplyPump2Running = True
                DischargePumpRunning = True

            # Unboxing Weather Type 
            if WeatherTypeIndex == 0:
                WeatherType = 'Sunny'
            elif WeatherTypeIndex == 1:
                WeatherType = 'Cloudy'
            else:
                WeatherType = 'Rainy'

            # Filling in the DataFrame with the Latest Data
            DataFrameLength = len(historicalDataframe)
            historicalDataframe.loc[DataFrameLength, 'Day'] = Day
            historicalDataframe.loc[DataFrameLength, 'Hour'] = Hour
            historicalDataframe.loc[DataFrameLength, 'Minutes'] = Minute
            historicalDataframe.loc[DataFrameLength, 'Weather'] = WeatherType
            historicalDataframe.loc[DataFrameLength, 'Temperature (C)'] = CurrentTemperature
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 1 Run Sts'] = SupplyPump1Running
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 1 Speed (%)'] = SupplyPump1Speed
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 1 Flow Rate (l/s)'] = SupplyPump1FlowRate
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 1 Power (KWh)'] = SupplyPump1Power
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 2 Run Sts'] = SupplyPump2Running
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 2 Speed (%)'] = SupplyPump2Speed
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 2 Flow Rate (l/s)'] = SupplyPump2FlowRate
            historicalDataframe.loc[DataFrameLength, 'Supply Pump 2 Power (KWh)'] = SupplyPump2Power
            historicalDataframe.loc[DataFrameLength, 'Discharge Pump Run Sts'] = DischargePumpRunning
            historicalDataframe.loc[DataFrameLength, 'Discharge Pump Speed (%)'] = DischargePumpSpeed
            historicalDataframe.loc[DataFrameLength, 'Discharge Pump Flow Rate (l/s)'] = DischargePumpFlowRate
            historicalDataframe.loc[DataFrameLength, 'Discharge Pump Power (KWh)'] = DischargePumpPower
            historicalDataframe.loc[DataFrameLength, 'Tank Level (Kl)'] = TankLevel
            historicalDataframe.loc[DataFrameLength, 'Current Power Usage (KWh)'] = CurrentPowerUsage
            historicalDataframe.loc[DataFrameLength, 'Daily Power Usage (KW)'] = DailyPowerUsage
            historicalDataframe.loc[DataFrameLength, 'Current Solar Generation (KWh)'] = CurrentSolarGeneration
            historicalDataframe.loc[DataFrameLength, 'Daily Solar Generation (KW)'] = DailySolarGeneration
            historicalDataframe.loc[DataFrameLength, 'Daily Power From Grid (KW)'] = DailyPowerFromTheGrid

            # Section to Enable Control of Pumps to Python
            
            # ------------------------    WRITING TO THE PLC TO CONTROL THE PUMPS -----------------------------------------------------
            #   1. EnableBit= The Enable Bit must be TRUE to enable writing to the PLC.
            #   2. RunPump1Bit= The Enable BOOLEAN AND RunPump1Bit BOOLEAN must be TRUE to enable running of Water Tank Supply Pump 1.
            #   3. RunPump2Bit= The Enable BOOLEAN AND RunPump2Bit BOOLEAN must be TRUE to enable running of Water Tank Supply Pump 2. 
            #   4. Discharge Pump can not be controlled.
            
            # ------------------ HERE IS AN EXAMPLE ---- 
           
            # -- Enable the control to the PLC
            #   if Hour > 12:
            #       EnableBit = True
            #   else:
            #   EnableBit = False

            # -- Setting the Pumps Running status
            #    if TankLevel < 15.0:
            #      RunPump1Bit = True
            #      RunPump2Bit = True
            #    elif TankLevel >= 15.0 and TankLevel < 20.0:
            #        RunPump1Bit = False
            #       RunPump2Bit = True
            #    elif TankLevel >= 20.0 and TankLevel < 25.0:
            #     RunPump1Bit = True
            #        RunPump2Bit = False
            #    else:
            #        RunPump1Bit = False
            #        RunPump2Bit = False
            

            # -- Conversion of Pumps Status bits to an INT, to write to the Modbus Register %MW500
            #   if EnableBit == True and RunPump1Bit == False  and RunPump2Bit == False:
            #        sentBit = 1
            #    elif EnableBit == True and RunPump1Bit == True  and RunPump2Bit == False:
            #        sentBit = 3
            #    elif EnableBit == True and RunPump1Bit == False  and RunPump2Bit == True:
            #        sentBit = 5
            #    elif EnableBit == True and RunPump1Bit == True  and RunPump2Bit == True:
            #        sentBit = 7
            #    else:
            sentBit = 0
                
            #  ------------------ END OF EXAMPLE  ---- 

            builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=Endian.Little)
            builder.add_16bit_int(sentBit)
            registers = builder.to_registers()
            self.client.write_registers(
            500,
            registers,
            unit=0)

            previousMinuteReturn = Minute


            ###

            return Day, previousMinuteReturn
        except:
            print("Failed to read")



# this has to be after all the class defintions
if __name__ == "__main__":
     ExecuteProcess()


    


    