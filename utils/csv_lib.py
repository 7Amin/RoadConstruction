import pandas as pd
from config import csv_data as csv_config


class CSV(object):
    def __init__(self, name, path):
        self.path = path
        self.name = name
        self.data = None
        self.size = 0

    def get_size_of_data(self):
        size_of_data = len(self.data)
        print("size of data for {} is {}".format(self.name, size_of_data))
        return size_of_data

    def read(self):
        print("read csv data is started with file {}".format(self.path))
        self.data = pd.read_csv(self.path, squeeze=True)
        self.size = len(self.data)
        print("read csv data is finished with file {}".format(self.path))

    def read_big_data_with_chunk_number(self, number):
        print("read csv data is started with file {}".format(self.path))
        print("chunk size is {}".format(csv_config.ChunkSize))
        for num, df in enumerate(pd.read_csv(self.path, chunksize=csv_config.ChunkSize), start=1):
            print("continue reading file page num is {}".format(num))
            if number == 0:
                self.data = df
                self.size = len(self.data)
                return
            number = number - 1
        print("read csv data is finished with file {}".format(self.path))

    def read_big_data(self):
        print("read csv data is started with file {}".format(self.path))
        print("chunk size is {}".format(csv_config.ChunkSize))
        for num, df in enumerate(pd.read_csv(self.path, chunksize=csv_config.ChunkSize), start=1):
            print("continue reading file page num is {}".format(num))
            self.data = df.append(self.data)
            self.size = len(self.data)
        print("read csv data is finished with file {}".format(self.path))

    def read_big_data_filter_with_key_values(self, keys, values):
        print("method {} is started ".format("read_big_data_filter_with_key_values"))
        print("read csv data is started with file {}".format(self.path))
        print("chunk size is {}".format(csv_config.ChunkSize))
        for key, value in zip(keys, values):
            print("the key is {} and the value is {}".format(key, value))
        for num, df in enumerate(pd.read_csv(self.path, chunksize=csv_config.ChunkSize), start=1):
            print("continue reading file page num is {}".format(num))
            for key, value in zip(keys, values):
                df = df[df[key] == value]
            self.data = df.append(self.data)
            self.size = len(self.data)
        print("method {} finished ".format("read_big_data_filter_with_key_values"))

    def read_big_data_filter_with_key_values_just_size(self, keys, values):
        print("method {} is started ".format("read_big_data_filter_with_key_values_just_size"))
        print("read csv data is started with file {}".format(self.path))
        print("chunk size is {}".format(csv_config.ChunkSize))
        for key, value in zip(keys, values):
            print("the key is {} and the value is {}".format(key, value))
        for num, df in enumerate(pd.read_csv(self.path, chunksize=csv_config.ChunkSize), start=1):
            print("continue reading file page num is {}".format(num))
            for key, value in zip(keys, values):
                df = df[df[key] == value]
                self.size = self.size + len(df)
                print("the size is {}".format(self.size))
        print("method {} finished ".format("read_big_data_filter_with_key_values"))


traffic_events_data = CSV(csv_config.TrafficEvents["name"], csv_config.TrafficEvents["path"])
weather_events_data = CSV(csv_config.WeatherEvents["name"], csv_config.WeatherEvents["path"])

