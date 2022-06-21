#####################################################
#           Import libraries/modules                #
#####################################################
import pandas as pd                    # 'usual' package to structure the data in dataframe
import numpy as np                     # 'usual' package for scientific calculations (e.g., natural log) 
import matplotlib.pyplot as plt        # 'usual' package to do plotting
import statsmodels.formula.api as smf  # 'usual' package to run regression with syntax similar to R
import statsmodels.api as sm           #  same as above but useful to run a generic regression
from numpy.linalg import inv
from scipy.stats import ttest_1samp

def main():
    hist_df = pd.read_csv('historicalDataFrame_Update_11_44am.csv')
    print(hist_df)
    print(hist_df.columns)
    breakpoint()

    water_shortage = hist_df['Discharge Pump Run Sts'].value_counts()
    print('water_shortage', water_shortage)

    tank_overfill = hist_df[(hist_df['Tank Level (Kl)'] >= 30)]['Tank Level (Kl)'].count()
    print('tank_overfill', tank_overfill)

    workload_solargenerate = hist_df[['Hour','Minutes','Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].groupby(['Hour','Minutes'])[['Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].mean()
    workload_solargenerate.plot()
    plt.show()

    sunny_workload_solargenerate = hist_df[hist_df['Weather'] == 'Sunny'][['Hour','Minutes','Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].reset_index(drop=True)
    sunny_workload_solargenerate.groupby(['Hour','Minutes'])[['Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].mean().plot()
    plt.show()

    cloudy_workload_solargenerate = hist_df[hist_df['Weather'] == 'Cloudy'][['Hour','Minutes','Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].reset_index(drop=True)
    cloudy_workload_solargenerate.groupby(['Hour','Minutes'])[['Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].mean().plot()
    plt.show()

    rainy_workload_solargenerate = hist_df[hist_df['Weather'] == 'Rainy'][['Hour','Minutes','Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].reset_index(drop=True)
    rainy_workload_solargenerate.groupby(['Hour','Minutes'])[['Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].mean().plot()
    plt.show()

    total_solar_wasted = hist_df[['Hour','Minutes','Current Power Usage (KWh)', 'Current Solar Generation (KWh)']].reset_index(drop=True)
    total_solar_wasted['waste'] = total_solar_wasted['Current Solar Generation (KWh)'] - total_solar_wasted['Current Power Usage (KWh)']
    total_solar_wasted = total_solar_wasted[(total_solar_wasted['waste'] >= 0)].reset_index(drop=True)
    total_waste = round(sum (total_solar_wasted['waste']) * 15 / 60 , 0)

    daily_average_water_demand = hist_df.groupby(['Hour','Minutes'])['Discharge Pump Flow Rate (l/s)'].mean() * 60 * 60
    round(daily_average_water_demand, 0 ).plot()
    plt.show()

    table_daily_cum_energy_solar = round(hist_df[hist_df['Hour'] == 24].reset_index(drop=True).groupby('Weather')[['Daily Solar Generation (KW)','Daily Power From Grid (KW)','Daily Power Usage (KW)']].mean(), 0)
    table_daily_cum_energy_solar['solar_percentage'] = round(table_daily_cum_energy_solar['Daily Solar Generation (KW)'] / table_daily_cum_energy_solar['Daily Power Usage (KW)'], 2)


    plt_daily_cum_energy_solar = round(hist_df[hist_df['Hour'] == 24].reset_index(drop=True).groupby('Weather')[['Daily Solar Generation (KW)','Daily Power From Grid (KW)']].mean(), 0).transpose()
    plt_daily_cum_energy_solar.plot.pie(subplots=True,figsize=(5, 5))
    plt_daily_cum_energy_solar.plot.pie(title = 'Percentage of average daily solar energy contribution', subplots=True,\
                   autopct='%1.1f%%', labels=None,\
                   shadow=True, startangle=0)
    plt.legend(['Daily Solar Generation (KW)','Daily Power From Grid (KW)'],bbox_to_anchor=(1,0), loc='best')
    plt.show()

    # the pump working decision made every 15mins
    tank_min = 1.5 #kl
    tank_max = 30 #kl
    pump1 = 5.199999809 # l/s
    pump2 = 1.75 # l/s
    discharge_pump_max =  4.599999905 # l/s
    discharge_pump_min = 1.840000033 # l/s while working 
    upper_limit = tank_max - (pump1 + pump2 - discharge_pump_min) * 15 * 60 / 1000
    lower_limit = tank_min + discharge_pump_max * 15 * 60 / 1000
    threshold_limit = [round(lower_limit, 0), round(upper_limit, 0)]
    print('threshold_limit', threshold_limit)



main()