import logging

import pendulum
from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator

import json
import psycopg2

from transformer import transform_weatherAPI


# [START instantiate_dag]
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['LearnDataEngineering'],
)
def ETLWeatherSimpleHttpOperator():

    @task()
    def extract():
        http_task = SimpleHttpOperator(
            task_id='http_request_task',
            http_conn_id='http_default',  # You need to configure an HTTP connection in Airflow
            endpoint='v1/current.json',
            method='GET',
            headers={},
            data={'Key': '', 'q': 'Berlin', 'aqi': 'no'}
        )

        response = http_task.execute(context=None)

        return json.loads(response)

    @task()
    def transform(weather_json: json):
        """
        A simple Transform task which takes in the API data and only extracts the location, wind,
        the temperature and time.
        """
        weather_str = json.dumps(weather_json)
        transformed_str = transform_weatherAPI(weather_str)

        # turn string into dictionary
        ex_dict = json.loads(transformed_str)

        # return ex_dict
        return ex_dict

    # LOAD: Save the data into Postgres database
    @task()
    def load(weather_data: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to the posgres database.
        """

        try:
            connection = psycopg2.connect(user="airflow",
                                          password="airflow",
                                          host="postgres",
                                          port="5432",
                                          database="WeatherData")
            cursor = connection.cursor()

            postgres_insert_query = """INSERT INTO temperature (location, temp_c, wind_kph, time) VALUES ( %s , %s, %s, %s);"""
            record_to_insert = (weather_data[0]["location"], weather_data[0]["temp_c"], weather_data[0]["wind_kph"],
                                weather_data[0]["timestamp"])
            cursor.execute(postgres_insert_query, record_to_insert)

            connection.commit()
            count = cursor.rowcount
            print(count, "Record inserted successfully into table")

        except (Exception, psycopg2.Error) as error:

            print("Failed to insert record into mobile table", error)

            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

            raise Exception(error)

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

    @task()
    def query_print(weather_data: dict):
        """
        #### Print task
        This just prints out the result into the log (even without instantiating a logger)
        """
        print(weather_data)

    weather_data = extract()
    weather_summary = transform(weather_data)
    load(weather_summary)
    query_print(weather_summary)


etl_weather_simple_http_operator = ETLWeatherSimpleHttpOperator()