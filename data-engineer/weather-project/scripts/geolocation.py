import pandas as pd
import requests
from get_engine import get_engine
from sqlalchemy import text

def fetch_geolocation_data():

    base_url = "http://api.geonames.org/searchJSON?"

    params = {
        "username" : "hendi",
        "country" : "ID",
        "featureClass" : "P"
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        print("Geolocation data fetched successfully.")
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def main():

    raw_data = fetch_geolocation_data()
    data = raw_data['geonames']
    df = pd.DataFrame(data)
    df = df[['name', 'lat','lng','population', 'adminName1']]
    print("Geolocation data processed into DataFrame. Shape:", df.shape)

    engine = get_engine()
    try:
        with engine.connect() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            connection.commit()
            print("Schema 'raw' created or already exists.")

        df.to_sql('geolocation', con=engine, schema='raw', if_exists='replace', index=False)
        print("Geolocation data loaded into the database.")

    except Exception as e:
        print(f"An error occurred while loading data into the database: {e}")

if __name__ == "__main__":
    main()