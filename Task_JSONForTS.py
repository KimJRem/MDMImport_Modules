# transform JSON to JSON format with {timestamp: value}
#class to transform the data from both APIs into the correct format for the TSR
import json

class JSONForTS:

#get data from RabbitMQ and transform to the format needed by the TSR

    def transform(self):
        # wetterdaten
        json_file = {"coord": {"lon": 9.5, "lat": 51.32},
                     "weather": [{"id": 803, "main": "Clouds", "description": "broken clouds", "icon": "04d"}],
                     "base": "stations",
                     "main": {"temp": 300.5, "pressure": 1015, "humidity": 47, "temp_min": 300.15, "temp_max": 301.15},
                     "visibility": 10000,
                     "wind": {"speed": 4.1, "deg": 200}, "clouds": {"all": 75}, "dt": 1532865000,
                     "sys": {"type": 1, "id": 4926, "message": 0.0021, "country": "DE", "sunrise": 1532835750,
                             "sunset": 1532891608},
                     "id": 2892518, "name": "Kassel", "cod": 200}

        json_new = json.dumps(json_file, sort_keys=True, indent=4)
        print('Json New: ')
        print(json_new)

        d1 = json.loads(json_new)
        print('Print load_file: ')
        print(d1)

        #make new dictionary and use values of other dict
        d2 = {}
        d2[d1]=

def main():
    task = JSONForTS()
    task.transform()

main()