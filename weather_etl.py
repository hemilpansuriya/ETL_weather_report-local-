import requests
import pandas as pd
import sqlite3
from datetime import datetime

def run_prod_etl():
    cities = [
        {"name": "Jetpur", "lat": 21.76, "lon": 70.62},
        {"name": "Rajkot", "lat": 22.30, "lon": 70.80},
        {"name": "Ankleshwar", "lat": 21.63, "lon": 73.00},
        {"name": "Toronto", "lat": 43.65, "lon": -79.38},
        {"name": "Ottawa", "lat": 45.42, "lon": -75.70},
        {"name": "Red Lake", "lat": 51.02, "lon": -93.83}
    ]

    all_data = []

    print(f"---Pipeline Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}---")

    for city in cities:
        try:
            #EXTRACT STEP
            url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&hourly=temperature_2m&timezone=auto"
            response = requests.get(url, timeout=10)
            response.raise_for_status
            data = response.json()

            #TRANSFORM STEP
            temp_df = pd.DataFrame({
                'city' : city['name'],
                'time': data['hourly']['time'],
                'temp_celsius' : data['hourly']['temperature_2m'],
                'ingestion_timestamp' : datetime.now()
            })

            all_data.append(temp_df)
            print(f"Successfull processed {city['name']}")
        except Exception as e:
            print(f"Failed to process {city['name']}")
    
    
    #LOAD (COMBINE AND LOAD) STEP
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        conn = sqlite3.connect('weather_warehouse.db')

        final_df.to_sql('city_forecasts', conn, if_exists='replace', index=False)
        conn.close()
        print(f"\n--- Load Complete: {len(final_df)} rows in database ---")

if __name__ == "__main__":
    run_prod_etl()



conn = sqlite3.connect('weather_warehouse.db')
query = "SELECT city, AVG(temp_celsius) as avg_temp FROM city_forecasts GROUP BY city"
results = pd.read_sql(query, conn)
print("Average Forecasted temperatures: ")
print(results)
conn.close()





#run this command to see output
#python -c "import sqlite3; import pandas as pd; conn = sqlite3.connect('weather_warehouse.db'); print(pd.read_sql('SELECT city, time, temp_celsius FROM city_forecasts LIMIT 5', conn)); conn.close()"

