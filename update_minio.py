import requests
import json
import concurrent.futures
import io
import os
from tqdm import tqdm
import datetime
import pandas as pd
from minio import Minio
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment Variables
TMDB_KEY = os.getenv('TMDB_KEY')
MINIO_SERVER = "minio.lab.sspcloud.fr"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_SESSION_TOKEN = os.getenv("MINIO_SESSION_TOKEN")
MINIO_BUCKET = "alimane"

# Initialize Minio client with credentials
client = Minio(
    MINIO_SERVER,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    session_token=MINIO_SESSION_TOKEN,
    secure=True
)

# Folder and file configurations
data_folder = 'diffusion/'
archive_folder = 'diffusion/TMDB_archive'
output_folder = 'diffusion/temp_data'
date_today_str = datetime.datetime.utcnow().date().isoformat()
json_save_object_name = f"{archive_folder}/{date_today_str}_TMDB_movies.ndjson"

def get_latest() -> int:
    """ Fetch the latest movie ID from TMDB API """
    url = f"https://api.themoviedb.org/3/movie/latest?api_key={TMDB_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    latest_id = response.json()['id']
    logging.info(f"Latest movie ID fetched: {latest_id}")
    return latest_id

def get_oldest(dataset_df) -> int:
    """ Retrieve the oldest movie ID from the dataset """
    oldest_id = dataset_df['id'].min()
    logging.info(f"Oldest movie ID: {oldest_id}")
    return oldest_id

def get_keywords(movie_id: int) -> dict:
    """ Fetch keywords for a movie from TMDB """
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/keywords?api_key={TMDB_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    keywords_data = response.json()
    logging.info(f"Keywords fetched for movie ID {movie_id}")
    return keywords_data

def process_movie_ids(movie_id: int, client: Minio, pbar: tqdm) -> None:
    """ Process each movie ID by fetching data and storing it in MinIO """
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_KEY}&language=en-US"
    response = requests.get(url)
    response.raise_for_status()
    movie_data = response.json()
    movie_data['keywords'] = ", ".join([k['name'] for k in get_keywords(movie_id)['keywords']])
    object_name = f"{output_folder}scrapeTMDB_movies_{movie_id}.ndjson"
    client.put_object(
        MINIO_BUCKET, object_name, data=io.BytesIO(json.dumps(movie_data).encode('utf-8')), length=len(json.dumps(movie_data))
    )
    logging.info(f"Processed and uploaded data for movie ID {movie_id}")
    pbar.update(1)
    
def process_movie_data(movie_data):
    """
    Ensure all data is in string format and handle null values.
    """
    # Assuming 'genres' might be a list of dictionaries, we extract genre names as a comma-separated string.
    if 'genres' in movie_data and isinstance(movie_data['genres'], list):
        movie_data['genres'] = ", ".join([genre['name'] for genre in movie_data['genres'] if 'name' in genre])

    # Convert all fields to string and handle None types
    for key, value in movie_data.items():
        if isinstance(value, list):  # If any other field is a list of dicts, handle similarly
            movie_data[key] = ", ".join([str(v) for v in value])
        elif value is None:
            movie_data[key] = None  # Or convert None to a string or a default value as required
        else:
            movie_data[key] = str(value)

    return movie_data

def combine_and_upload(client: Minio, output_folder: str, archive_folder: str):
    """ Combine all individual movie data files into one and upload to MinIO """
    objects = client.list_objects(MINIO_BUCKET, prefix=output_folder, recursive=True)
    combined_data = []
    for obj in objects:
        response = client.get_object(MINIO_BUCKET, obj.object_name)
        combined_data.extend([json.loads(line.decode('utf-8')) for line in response.stream(32*1024)])
    client.put_object(
        MINIO_BUCKET, f"{archive_folder}/combined_{date_today_str}.ndjson",
        data=io.BytesIO(json.dumps(combined_data).encode('utf-8')), length=len(json.dumps(combined_data))
    )
    logging.info("Combined data uploaded to MinIO")

def load_and_update_dataset(client: Minio, original_file: str, update_file: str):
    response = client.get_object(MINIO_BUCKET, original_file)
    original_data = pd.read_parquet(io.BytesIO(response.data))

    response = client.get_object(MINIO_BUCKET, update_file)
    update_data = pd.read_json(io.BytesIO(response.data))

    update_data = update_data.apply(process_movie_data, axis=1)
    updated_df = pd.concat([original_data, update_data]).drop_duplicates(subset='id').reset_index(drop=True)

    buffer = io.BytesIO()
    updated_df.to_parquet(buffer, index=False)
    buffer.seek(0)  # Rewind the buffer after writing
    client.put_object(MINIO_BUCKET, original_file, data=buffer.getvalue(), length=len(buffer.getvalue()))
    logging.info("Dataset updated and saved back to MinIO")



    
        
def executor():
    """ Main executor function """
    dataset_df = pd.read_parquet(f'https://{MINIO_SERVER}/{MINIO_BUCKET}/diffusion/TMDB_movies.parquet')
    latest = get_latest()
    oldest = get_oldest(dataset_df)
    movie_ids_list = list(range(latest-400, latest + 1))
    with tqdm(total=len(movie_ids_list)) as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_movie_ids, mid, client, pbar) for mid in movie_ids_list]
            concurrent.futures.wait(futures)
    combine_and_upload(client, output_folder, archive_folder)
    load_and_update_dataset(client, 'diffusion/TMDB_movies.parquet', f'{archive_folder}/combined_{date_today_str}.ndjson')

if __name__ == "__main__":
    executor()
