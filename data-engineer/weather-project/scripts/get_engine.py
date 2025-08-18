from sqlalchemy import create_engine

def get_engine():
    engine = create_engine('postgresql+psycopg2://noc:star_net2016@172.18.0.100/dwh_mega')
    return engine

if __name__ == "__main__":
    get_engine()