import pandas as pd
import requests
from get_engine import get_engine
from sqlalchemy import text, inspect


def get_geolocation_from_db():
    engine = get_engine()

    query = "SELECT * FROM raw.geolocation"

    try:
        with engine.connect() as connection:
            df = pd.read_sql(query, con=connection)
            print("Geolocation data fetched from database. Shape:", df.shape)
            return df
    
    except Exception as e:
        print(f"Error fetching geolocation data: {e}")
        return None


def fetch_weather_data(latitude, longitude):

    base_url = "https://api.open-meteo.com/v1/forecast?"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Asia/Jakarta",
        "forecast_days": 7
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching weather data: {response.status_code} - {response.text}")
        return None

def load_data_to_db(df, engine):
    df['time'] = pd.to_datetime(df['time'])

    # Tentukan rentang tanggal
    min_date = df['time'].min()
    max_date = df['time'].max()

    

    # Membuat query delete
    delete_query = text("""
        DELETE FROM raw.weather_data
        WHERE time::timestamp >= :min_date AND time::timestamp <= :max_date
    """)

    try:
        with engine.connect() as connection:
            with connection.begin() as transaction:
                inspector = inspect(engine)
                if not inspector.has_table('weather_data', schema='raw'):
                    print("Table 'raw.weather_data' does not exist. Creating table...")
                    df.to_sql('weather_data', schema='raw', con=connection, if_exists='replace', index=False)
                    print("Table 'raw.weather_data' created and data loaded.")
                
                print("Deleting existing data in the specified date range...")
                result = connection.execute(delete_query, {'min_date': min_date, 'max_date': max_date})
                print(f"Deleted {result.rowcount} rows from raw.weather_data.")

                # Load new data
                print(f"Loading {len(df)} new rows into raw.weather_data...")
                df.to_sql('weather_data', schema='raw', con=connection, if_exists='append', index=False)
            print("New data loaded successfully.")
                
    except Exception as e:
        print(f"Error during data loading: {e}")


def main():
    geolocation_df = get_geolocation_from_db()
    all_weather_data = []

    for index, data_geo in geolocation_df.iterrows():
        latitude = data_geo['lat']
        longitude = data_geo['lng']
        weather_data = fetch_weather_data(latitude, longitude)

        index = index + 1
        total_rate = len(geolocation_df)
        process_rate = (index / total_rate) * 100
        print(f"Processing {process_rate:.2f}% of geolocation data...")
        if weather_data:
            daily_data = weather_data['daily']
            df = pd.DataFrame(daily_data)
            df['name'] = data_geo['name']
            all_weather_data.append(df)
        
        else:
            print(f"Failed to fetch weather data for {data_geo['name']} at lat: {latitude}, lng: {longitude}")

    final_df = pd.concat(all_weather_data, ignore_index=True)

    #========== Load to database ==========#
    engine = get_engine()

    # try:
    #     with engine.connect() as connection:
    #         final_df.to_sql('weather_data',schema='raw', con=connection, if_exists='replace', index=False)
    #         print("Weather data loaded to database successfully.")
    # except Exception as e:
    #     print(f"Error loading weather data to database: {e}")
    load_data_to_db(final_df, engine)

if __name__ == "__main__":
    main()