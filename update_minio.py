import requests
import json
import concurrent.futures
import io
import os
from tqdm import tqdm
import datetime
import pandas as pd
from minio import Minio

# Environment Variables
TMDB_KEY = os.getenv('TMDB_KEY')
MINIO_SERVER = "minio.lab.sspcloud.fr"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_BUCKET = "alimane"

# Initialize Minio client with credentials
client = Minio(
    MINIO_SERVER,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
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
    url = "https://api.themoviedb.org/3/movie/latest"
    headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_KEY}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
    return response.json()['id']

def get_oldest(dataset_df) -> int:
    """ Retrieve the oldest movie ID from the dataset """
    return dataset_df['id'].min()

def get_keywords(movie_id: int) -> dict:
    """ Fetch keywords for a movie from TMDB """
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/keywords"
    headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_KEY}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def process_movie_ids(movie_id: int, client: Minio, pbar: tqdm) -> None:
    """ Process each movie ID by fetching data and storing it in MinIO """
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
    headers = {"accept": "application/json", "Authorization": f"Bearer {TMDB_KEY}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    movie_data = response.json()
    movie_data['keywords'] = ", ".join([k['name'] for k in get_keywords(movie_id)['keywords']])

    # Serialize and upload to MinIO
    object_name = f"{output_folder}scrapeTMDB_movies_{movie_id}.ndjson"
    client.put_object(
        MINIO_BUCKET, object_name, data=io.BytesIO(json.dumps(movie_data).encode('utf-8')), length=len(json.dumps(movie_data))
    )
    pbar.update(1)

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

def load_and_update_dataset(client: Minio, original_file: str, update_file: str):
    """ Load the original dataset, update it with new data, and save back to MinIO """
    original_data = pd.read_parquet(io.BytesIO(client.get_object(MINIO_BUCKET, original_file).data))
    update_data = pd.read_json(io.BytesIO(client.get_object(MINIO_BUCKET, update_file).data))
    updated_df = pd.concat([original_data, update_data]).drop_duplicates(subset='id').reset_index(drop=True)
    buffer = io.BytesIO()
    updated_df.to_parquet(buffer, index=False)
    client.put_object(MINIO_BUCKET, original_file, data=buffer.getvalue(), length=buffer.tell())

def executor():
    """ Main executor function """
    logger = setup_logger()
    logger.info('Starting Executor')
    dataset_df = pd.read_parquet(f'https://{MINIO_SERVER}/{MINIO_BUCKET}/diffusion/TMDB_movies.parquet')
    latest = get_latest()
    oldest = get_oldest(dataset_df)

    movie_ids_list = list(range(oldest, latest + 1))
    logger.info(f"Total Movies to Process: {len(movie_ids_list)}")
    
    with tqdm(total=len(movie_ids_list)) as pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_movie_ids, mid, client, pbar) for mid in movie_ids_list]
            concurrent.futures.wait(futures)

    combine_and_upload(client, output_folder, archive_folder)
    load_and_update_dataset(client, 'diffusion/TMDB_movies.parquet', f'{archive_folder}/combined_{date_today_str}.ndjson')

    logger.info('Completed Executor')

if __name__ == "__main__":
    executor()
