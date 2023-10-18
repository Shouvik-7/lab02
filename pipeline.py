import pandas as pd
import numpy as np
from sqlalchemy import create_engine


def get_sex(x):
    li = str(x).split()
    if len(li) > 1:
        return li[1]
    else:
        return "Unknown"
def get_gender_upon_outcome(x):
  li = str(x).split()
  if len(li) > 1:
    return li[0]
  else:
    return "Unknown"

def read_data(source):
    return pd.read_csv(source)

def save_data(data, target):
    data.to_csv(target, index=False)

def get_animal_dim(data):
    animal_dim = data[['animal_id', 'animal_name', 'dob', 'unix_dob', 'animal_type', 'breed', 'color', 'sex']]
    animal_dim = animal_dim.drop_duplicates(subset=['animal_id'], keep='first')
    return animal_dim

def get_date_dim(data):
    date_dim = data[['ts','unix_date']]
    date_dim['ts'] = pd.to_datetime(date_dim['ts'])
    date_dim['year'] = date_dim['ts'].apply(lambda x: int(x.year))
    date_dim['month'] = date_dim['ts'].apply(lambda x: int(x.month))
    date_dim['day'] = date_dim['ts'].apply(lambda x: int(x.day))
    date_dim['hour'] = date_dim['ts'].apply(lambda x: int(x.hour))
    date_dim['minute'] = date_dim['ts'].apply(lambda x: int(x.minute))
    date_dim['second'] = date_dim['ts'].apply(lambda x: int(x.second))
    date_dim = date_dim[['unix_date', 'year', 'month', 'day', 'hour', 'minute', 'second']]
    return date_dim

def get_outcome_dim(data):
    data['outcome_type'] = data['outcome_type'].apply(lambda x: str(x))
    outcome_type_li = data['outcome_type'].unique()
    outcome_type_dim = pd.DataFrame()
    outcome_type_dim['outcome_type'] = outcome_type_li
    return outcome_type_dim

def create_outcome_fct(data, date_df, outcome_df):
    data['date_id'] = date_df['date_id']
    outcome_df = outcome_df.set_index('outcome_type')
    outcome_map = outcome_df.to_dict()['outcome_type_id']
    data['outcome_type_id'] = data['outcome_type'].replace(outcome_map)
    return data

def load_outcome_fct(data):
    data2 = data[['animal_id','outcome_type_id','date_id','outcome_subtype','gender_upon_outcome']]
    #data2['outcome_id'] = [i for i in range(len(data2))]
    db_url = "postgresql+psycopg2://shouvik:hunter2@db/shelter"
    conn2 = create_engine(db_url)
    data2.to_sql('outcome_fct',conn2, if_exists="append", index=False)

def load_data(animal_dim, date_dim, outcome_dim):

    db_url = "postgresql+psycopg2://shouvik:hunter2@db/shelter"
    conn = create_engine(db_url)
    animal_dim.to_sql("animal_dim", conn, if_exists="append", index=False)
    date_dim.to_sql("date_dim", conn, if_exists="append", index=False)
    outcome_dim.to_sql("outcome_dim", conn, if_exists="append", index=False)
    date_df = pd.read_sql_table('date_dim',conn)
    outcome_df = pd.read_sql_table('outcome_dim', conn)
    return date_df, outcome_df


def transform_data(data):
    data.rename(columns={'Animal ID': 'animal_id', 'Name': 'animal_name', 'DateTime': 'ts', 'Date of Birth': 'dob',
                         'Outcome Type': 'outcome_type', 'Outcome Subtype': 'outcome_subtype',
                         'Animal Type': 'animal_type', 'Sex upon Outcome': 'sex_upon', 'Age upon Outcome': 'age',
                         'Breed': 'breed', 'Color': 'color'}, inplace=True)

    data['sex'] = data['sex_upon'].apply(get_sex) # fetch sex from sex_upon_outcome
    data['gender_upon_outcome'] = data['sex_upon'].apply(get_gender_upon_outcome) # fetch gender_upon_outcome from sex_upon_outcome
    data['animal_name'] = data['animal_name'].apply(lambda x: str(x).replace('*', '')) # remove * from names
    data['dob'] = pd.to_datetime(data['dob'])
    data['ts'] = pd.to_datetime(data['ts'])
    data['unix_dob'] = data['dob'].apply(lambda x: pd.Timestamp(x).timestamp())
    data['unix_date'] = data['ts'].apply(lambda x: pd.Timestamp(x).timestamp())
    data = data[['animal_id', 'animal_name', 'ts', 'unix_date', 'dob', 'unix_dob', 'outcome_type', 'outcome_subtype', 'animal_type', 'breed', 'color', 'sex', 'gender_upon_outcome']]

    animal_dim = get_animal_dim(data)
    date_dim = get_date_dim(data)
    outcome_dim = get_outcome_dim(data)
    return data, animal_dim, date_dim, outcome_dim


if __name__ == '__main__':
    print("starting")
    # n = len(sys.argv)
    # filename = sys.argv[1]
    targetname = 'target.csv'
    filename = 'data.csv'
    df = read_data(filename)
    df, animal_dim, date_dim, outcome_dim = transform_data(df)
    date_df, outcome_df = load_data(animal_dim, date_dim, outcome_dim)
    df = create_outcome_fct(df,date_df, outcome_df)
    load_outcome_fct(df)
    save_data(df, targetname)
    print(df.head(20))
    print("Complete")
