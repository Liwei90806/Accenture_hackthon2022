import copy

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def average_energy_generation_consumption_by_hour(dataframe : pd.DataFrame):
    print(dataframe)
    twenty_four_hours = np.arange(0, 24)
    power_generation_consumption_items = ["Current Solar Generation (KWh)", "Current Power Usage (KWh)", "Current Power From Grid (KWh)", "Battery Power Failed To Charge (KWh)", "Battery Power Stored (KWh)"]
    df = pd.DataFrame(index=twenty_four_hours, columns=power_generation_consumption_items)
    for hour in twenty_four_hours:
        for item in power_generation_consumption_items:
            df.loc[hour, item] = dataframe[dataframe["Hour"] == hour][item].mean()
    return df

def plot_energy_consumption_breakdown(dataframe : pd.DataFrame, weather):
    dataframe.plot()
    plt.xlabel("Hour of day")
    plt.ylabel("Power (KW)")
    plt.title(weather + " Days (Averaged) Energy Consumption Breakdown")
    plt.show()

def plot_energy_over_one_day(dataframe : pd.DataFrame, weather):
    df = copy.deepcopy(dataframe)
    df.reset_index(inplace=True)
    metrics_to_show = ["Current Solar Generation (KWh)", "Current Power Usage (KWh)", "Current Power From Grid (KWh)", "Battery Power Failed To Charge (KWh)", "Battery Power Stored (KWh)"]
    df = df.loc[:, ["Day"] + metrics_to_show]
    print(df)
    grouped = df.groupby("Day")
    for group_name, indexes in grouped.groups.items():
        df.iloc[indexes].plot(y=metrics_to_show)
        plt.xlabel("Sample Points")
        plt.ylabel("Power (KW)")
        plt.title("Typical %s Day Energy Consumption Breakdown" % weather)
        plt.show()
        break

def plot_solar_percantage_and_efficiency(dataframe : pd.DataFrame, weather):
    def calculate_power_utilization(battery_failed_to_charge, current_solar_generation):
        if current_solar_generation != 0:
            return 100.0 * (1 - battery_failed_to_charge / current_solar_generation)
        else:
            return 1
    dataframe["Percantage Of Power From Solar"] = 100.0 * dataframe["Current Solar Generation (KWh)"] / dataframe["Current Power Usage (KWh)"]
    # dataframe["Solar Power Utilization"] = 100.0 * (1 - (dataframe["Battery Power Failed To Charge (KWh)"] / dataframe["Current Solar Generation (KWh)"]))
    dataframe["Solar Power Utilization"] = dataframe.apply(lambda x: calculate_power_utilization(x["Battery Power Failed To Charge (KWh)"], x["Current Solar Generation (KWh)"]), axis=1)
    dataframe["Percantage Of Power From Solar"].clip(lower=0, upper=100, inplace=True)
    dataframe["Solar Power Utilization"].clip(lower=0, upper=100, inplace=True)
    dataframe.plot(y=["Percantage Of Power From Solar", "Solar Power Utilization"])
    plt.ylabel("Percantage")
    plt.xlabel("Hour of day")
    plt.title(weather + " Days Solar Utilization And Efficiency")
    plt.show()

def group_energy_generation_consumption_by_weather(dataframe : pd.DataFrame):
    grouped = dataframe.groupby("Weather")
    grouped_dataframes = {}

    for group_name, indexes in grouped.groups.items():
        df = copy.deepcopy(dataframe.iloc[indexes])
        df.reset_index(inplace=True)
        grouped_dataframes[group_name] = df

    return grouped_dataframes

def visualize_energy_usage(dataframe : pd.DataFrame):
    dataframes_grouped_by_weather = group_energy_generation_consumption_by_weather(dataframe)
    for weather, df in dataframes_grouped_by_weather.items():
        averaged = average_energy_generation_consumption_by_hour(df)
        plot_energy_consumption_breakdown(averaged, weather)
        plot_solar_percantage_and_efficiency(averaged, weather)
        plot_energy_over_one_day(df, weather)

def calculate_auxiliary_data(dataframe : pd.DataFrame):
    def calculate_power_from_grid(solar_generation, power_usage):
        if solar_generation <= power_usage:
            return power_usage - solar_generation
        else:
            return 0
    dataframe["Current Power From Grid (KWh)"] = dataframe.apply(lambda x: calculate_power_from_grid(x["Current Solar Generation (KWh)"], x["Current Power Usage (KWh)"]), axis=1)
    dataframe["Battery Power Failed To Charge (KWh)"] = 0
    dataframe["Battery Power Stored (KWh)"] = 0
    return dataframe

def plot_water_outage_occurances(dataframe : pd.DataFrame):
    water_outage_occurances = dataframe[dataframe["Tank Level (Kl)"] <= 1.5]
    # First row of data always have water tank level at 0, no matter what strategy is applied
    water_outage_occurances = water_outage_occurances.iloc[1:, :]
    water_outage_occurances["Day"].hist(bins=82)
    plt.title("Water Outage Occurances Over 82 Days")
    plt.xlabel("Days")
    plt.ylabel("Water Outage Occurances Each Day")
    plt.show()

    water_overflow_occurances = dataframe[dataframe["Tank Level (Kl)"] >= 30]
    water_overflow_occurances["Day"].hist(bins=82)
    plt.title("Water Tank Overflow Occurances Over 82 Days")
    plt.xlabel("Days")
    plt.ylabel("Water Tank Overflow Occurances Each Day")
    plt.show()

def simluate_strategy(dataframe : pd.DataFrame, battery_capacity=0, solar_panel_multiplier=1):
    """
        Use solar power whenever there is any excess.
        Excess solar power means solar power minus power used by discharge pump
        No solar power: Close Pump unless water tank threshold met
        Excess solar power <= 15KW: Open Pump 2
        Excess solar power <= 45KW: Open Pump 1
        Excess solar power >= 45KW: Open Pump 1 and Pump 2
        Excess solar power >= 60KW: No battery to store it unfortunately...
        Water Tank Low Threshold: 5.7 KL: 1.5 + 4.599999905 * 15 * 60 -> 5.639KL
        Water Tank High Threshold: 25.3: 30 - (5.199999809 + 1.75 - 1.840000033) * 15 * 60 -> 25.401KL
    """
    water_tank_low_threshold = 5.7
    water_tank_high_threshold = 25.3
    pump_one_power = 45
    pump_two_power = 15
    discharge_pump_max_power = 40
    pump_one_flow = 5.199999809
    pump_two_flow = 1.75
    discharge_pump_max_flow = 4.599999905

    assert solar_panel_multiplier >= 1
    dataframe["Current Solar Generation (KWh)"] = dataframe["Current Solar Generation (KWh)"] * solar_panel_multiplier
    dataframe["Battery Power Stored (KWh)"] = 0
    dataframe["Current Power From Grid (KWh)"] = 0
    dataframe["Battery Power Failed To Charge (KWh)"] = 0
    print(dataframe)

    result_df = copy.deepcopy(dataframe)
    prev_row = None

    for index, row in dataframe.iterrows():
        if row["Discharge Pump Power (KWh)"] == 0:
            row["Discharge Pump Power (KWh)"] = row["Discharge Pump Speed (%)"] * 0.01 * discharge_pump_max_power
            row["Discharge Pump Flow Rate (l/s)"] = row["Discharge Pump Speed (%)"] * 0.01 * discharge_pump_max_flow

        if prev_row is None:
            prev_row = row
            result_df.iloc[index] = row
            continue

        water_flow = prev_row["Supply Pump 1 Flow Rate (l/s)"] + prev_row["Supply Pump 2 Flow Rate (l/s)"] - prev_row["Discharge Pump Flow Rate (l/s)"]
        row["Tank Level (Kl)"] = prev_row["Tank Level (Kl)"] + water_flow * 60 * 15 * 0.001 # 0.001 because of l/s * s -> KL

        assert 1.5 < row["Tank Level (Kl)"] < 30, "Strategy failed to prevent water outage... Prev Row: %s" % prev_row

        pump_status = ()
        excess_solar_power = row["Current Solar Generation (KWh)"] - row["Discharge Pump Power (KWh)"]
        if excess_solar_power >= 45:
            pump_status = (True, True)
        elif excess_solar_power > 15:
            pump_status = (True, False)
        elif excess_solar_power > 0:
            pump_status = (False, True)
        else:
            pump_status = (False, False)
        if row["Tank Level (Kl)"] >= water_tank_high_threshold:
            pump_status = (False, False)
        elif row["Tank Level (Kl)"] <= water_tank_low_threshold:
            pump_status = (True, False)
        power_consumption = row["Discharge Pump Power (KWh)"]
        pump_one, pump_two = pump_status
        if pump_one is True:
            row["Supply Pump 1 Run Sts"] = True
            row["Supply Pump 1 Flow Rate (l/s)"] = pump_one_flow
            power_consumption += pump_one_power
        else:
            row["Supply Pump 1 Run Sts"] = False
            row["Supply Pump 1 Flow Rate (l/s)"] = 0
        if pump_two is True:
            row["Supply Pump 2 Run Sts"] = True
            row["Supply Pump 2 Flow Rate (l/s)"] = pump_two_flow
            power_consumption += pump_two_power
        else:
            row["Supply Pump 2 Run Sts"] = False
            row["Supply Pump 2 Flow Rate (l/s)"] = 0
        row["Current Power Usage (KWh)"] = power_consumption

        battery_capacity_to_change = (row["Current Solar Generation (KWh)"] - row["Current Power Usage (KWh)"]) * 0.25 # Unit in KWh
        battery_capacity_actualy_changed = None
        if battery_capacity_to_change < 0:
            # Discharge battery
            if (prev_row["Battery Power Stored (KWh)"] + battery_capacity_to_change) < 0:
                battery_capacity_actualy_changed = -1 * prev_row["Battery Power Stored (KWh)"]
                row["Battery Power Stored (KWh)"] = 0
            else:
                battery_capacity_actualy_changed = battery_capacity_to_change
                row["Battery Power Stored (KWh)"] = prev_row["Battery Power Stored (KWh)"] + battery_capacity_actualy_changed
            row["Current Power From Grid (KWh)"] = (battery_capacity_actualy_changed - battery_capacity_to_change) * 4
        else:
            # Charge battery
            if (prev_row["Battery Power Stored (KWh)"] + battery_capacity_to_change > battery_capacity):
                battery_capacity_actualy_changed = battery_capacity - prev_row["Battery Power Stored (KWh)"]
                row["Battery Power Stored (KWh)"] = battery_capacity
                row["Battery Power Failed To Charge (KWh)"] = battery_capacity_to_change - battery_capacity_actualy_changed
            else:
                battery_capacity_actualy_changed = battery_capacity_to_change
                row["Battery Power Stored (KWh)"] = prev_row["Battery Power Stored (KWh)"] + battery_capacity_actualy_changed
            row["Current Power From Grid (KWh)"] = 0

        prev_row = row
        result_df.iloc[index] = row
        print(index)
    print(result_df)

    return result_df

def visualize_provided_data(dataframe):
    dataframe = calculate_auxiliary_data(dataframe)
    visualize_energy_usage(dataframe)
    plot_water_outage_occurances(dataframe)

def simulation_with_strategy_and_visualize(dataframe):
    battery_capacity = 500 # Suppose we use battery with 500 KWh capacity
    solar_panel_multiplier = 2 # Suppose we have double the amount of solar panels
    simulated_df = simluate_strategy(dataframe, battery_capacity, solar_panel_multiplier)

    visualize_energy_usage(simulated_df)
    plot_water_outage_occurances(simulated_df)

    file_name = "SimulatedDataframe_%s_%s.csv" % (battery_capacity, solar_panel_multiplier)
    simulated_df.to_csv(file_name)

def main():
    # Read historical data provided and visualize it.
    # See that it has bad utilization of solar power, and that it has days with water outages
    dataframe = pd.read_csv("historicalDataFrame_Update_11_44am.csv")
    # visualize_provided_data(dataframe)

    # Optionally, run the simulation where our proposed strategy is applied and then visualize the result and compare
    simulation_with_strategy_and_visualize(dataframe)

    # Otherwise, we could load csv file with data saved in previous runs of simulations.
    # simulated = pd.read_csv("SimulatedDataframe_50_2.csv")
    # visualize_provided_data(simulated)

if __name__ == "__main__":
    main()
