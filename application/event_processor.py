from utils import csv_lib as csv_util, plot as plot_util
from config import state as state_config


class EventProcessor(object):
    def __init__(self):
        self.traffic_data = csv_util.traffic_events_data
        self.weather_data = csv_util.weather_events_data
        self.traffic_data_for_states = []
        self.traffic_data_for_states_sorted = []
        self.weather_data_for_states = []
        self.weather_data_for_states_sorted = []

    def fill_data_of_state(self, state):
        self.traffic_data.read_big_data_filter_with_key_values(
            keys=['Type', 'State'],
            values=['Construction', state]
        )
        self.traffic_data.get_size_of_data()

        self.weather_data.read_big_data_filter_with_key_values(keys=['State'], values=[state])
        self.weather_data.get_size_of_data()

    def fill_data(self):
        self.traffic_data.read_big_data_filter_with_key_values(
            keys=['Type'],
            values=['Construction']
        )
        self.traffic_data.get_size_of_data()

        self.weather_data.read_big_data()
        self.weather_data.get_size_of_data()

        for state in state_config.USA_STATES:
            number_of_traffic_data = len(self.traffic_data.data[self.traffic_data.data["State"]
                                                                == state_config.USA_STATES[state]["SHORT_KEY"]])
            self.traffic_data_for_states.append({"State": state, "Number": number_of_traffic_data})

            number_of_weather_data = len(self.weather_data.data[self.weather_data.data["State"]
                                                                == state_config.USA_STATES[state]["SHORT_KEY"]])
            self.weather_data_for_states.append({"State": state, "Number": number_of_weather_data})

        self.traffic_data_for_states_sorted = sorted(self.traffic_data_for_states, key=lambda data: data["Number"])
        self.weather_data_for_states_sorted = sorted(self.weather_data_for_states, key=lambda data: data["Number"])

        plot_util.plot_2d_list(self.traffic_data_for_states_sorted,
                               "Number",
                               "State",
                               "Distribution of construction data base on state",
                               "Number",
                               "State",
                               True,
                               "ConstructionDistribution.png")

        plot_util.plot_2d_list(self.weather_data_for_states_sorted,
                               "Number",
                               "State",
                               "Distribution of weather data base on state",
                               "Number",
                               "State",
                               True,
                               "WeatherDistribution.png")

        traffic_data_group = self.traffic_data.data.groupby("State")
        print(len(traffic_data_group))
        weather_data_group = self.weather_data.data.groupby("Type")
        print(len(weather_data_group))


event_processor = EventProcessor()
event_processor.fill_data()
# event_processor.fill_data_of_state(state_config.USA_STATES['Ohio']["SHORT_KEY"])
