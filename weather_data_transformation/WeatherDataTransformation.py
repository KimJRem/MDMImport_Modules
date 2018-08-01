import datetime

from AnalysisHelperClass import *
# https://stackoverflow.com/questions/50813108/get-transferred-file-name-in-rabbitmq-using-python-pika
# for transferring csv files
from rabbitmq import RabbitMQConsumer

setup_logging()


class WeatherDataTransformation:

    def __init__(self, df):
        self.df = df

    # select important columns, rename columns, convert Kelvin to Celsius, convert Date to readable format
    def weather_api_data_prep(self):
        # extract only relevant columns
        df_selected_columns = self.df[
            ['dt', 'weather_0_main', 'weather_0_description', 'main_temp', 'main_temp_min', 'main_temp_max',
             'main_pressure', 'main_humidity', 'sys_sunrise', 'sys_sunset', 'wind_speed', 'wind_deg',
             'clouds_all', 'visibility', 'rain_3h', 'snow_3h']]
        # rename columns
        df_selected_columns = df_selected_columns.rename(
            columns={"dt": "Date_unix", "weather_0_main": "General_description",
                     "weather_0_description": "Short_description",
                     "main_temp": "Temperature", "main_temp_min": "Min_Temperature",
                     "main_temp_max": "Max_Temperature", "main_pressure": "Pressure",
                     "main_humidity": "Humidity", "sys_sunrise": "Sunrise",
                     "sys_sunset": "Sunset", "wind_speed": "Wind_speed",
                     "wind_deg": "Wind_direction", "clouds_all": "Clouds",
                     "visibility": "Visibility", "rain_3h": "Rain_last3h", "snow_3h": "Snow_last3h"})
        # create three new columns with °C instead of Kelvin, alternatively get Celsius directly from API
        df_selected_columns['Temp_Celsius'] = (df_selected_columns.Temperature - 273.15)
        df_selected_columns['Min_Temperature_Celsius'] = (df_selected_columns.Min_Temperature - 273.15)
        df_selected_columns['Max_Temperature_Celsius'] = (df_selected_columns.Max_Temperature - 273.15)
        # convert date to readable format, should be the same format as the MDM data
        df_selected_columns['Date'] = df_selected_columns.apply(
            lambda row: datetime.datetime.utcfromtimestamp(row['Date_unix']).replace(tzinfo=datetime.timezone.utc),
            axis=1)
        df_selected_columns['Date'] = pd.to_datetime(df_selected_columns['Date']).dt.tz_convert('Europe/Berlin')
        return df_selected_columns


# function for min and max value for every column of the Dataframe
def all_min_max(df):
    return [df.max(), df.min()]


# function for min, max value of a specific column
def min_max(df, column_name):
    return [df[column_name].max(), df[column_name].min()]


def all_describe(df):
    print('Statistical overview of the weather data: ')
    return df.describe()


# Statistical overview of selected columns
def describe(df, column_name):
    print('Statistical overview of the column %s: ' % column_name)
    return df[column_name].describe()


consumer_config = json.dumps({
    "exchangeName": "topic_datas",
    "host": "127.0.0.1",
    "routingKey": "24",
    "exchangeType": "topic",
    "queueName": "12",
    "exchangeOptions": {
        "passive": True,
        "durable": True,
        "autoDelete": True,
        "internal": False
    },
    "queueOptions": {
        "passive": True,
        "durable": True,
        "exclusive": True,
        "autoDelete": True
    }
})


def main():
    with RabbitMQConsumer(json.loads(consumer_config)) as consumer:
        consumer.consume(extract_data)


def extract_data(csv_data):
    analysis_task = AnalysisHelperClass()
    # decode csv file
    data = analysis_task.decodeCsv(csv_data)
    # turn csv file into DF
    df = analysis_task.csvToDF(data)
    print('Print DF: ')
    print(df)
    # do statistical overview
    task = WeatherDataTransformation(df)
    data_prep_df = task.weather_api_data_prep()
    print(data_prep_df)

    extract_min_max(data_prep_df)
    extract_statistical_overview(data_prep_df)


def extract_min_max(data_prep):
    all_min_max_value = all_min_max(data_prep)
    print(
        'The maximum values are: \n%s. \nThe minimum values are: \n%s.' % (all_min_max_value[0], all_min_max_value[1]))

    max_min_value = min_max(data_prep, 'Wind_speed')
    print(
        'The maximum value of the selected column is: \n%s. \nThe minimum value of the selected column is: \n%s.' % (
            max_min_value[0], max_min_value[1]))


def extract_statistical_overview(data_prep):
    stats = all_describe(data_prep)
    print(stats)
    column_stats = describe(data_prep, 'Temp_Celsius')
    print(column_stats)


main()
