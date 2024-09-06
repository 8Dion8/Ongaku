import zipfile
import os
import re
import requests
import redis
import glob
import json
import tqdm
import datetime
from collections import Counter
import pycountry


class ZipParser:
    def __init__(self):
        self.extract_re = re.compile(r".*/Streaming_History_Audio_.*\.json")
        self.redis_port = 8301
        self.ip_to_location_hash = {}
        self.redis_connect()
        pass

    def redis_connect(self):
        self.redis = redis.Redis(host="localhost", port=self.redis_port)
        print("Connected to Redis")

    def extract_json_from_zip(self, zip_path):
        os.makedirs("tmp_extracted", exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for file in zip_ref.namelist():
                print(file)
                if self.extract_re.match(file):
                    zip_ref.extract(file, "tmp_extracted")
                    print(f"Extracted {file}")

    def ip_to_location(self, ip):
        if ip in self.ip_to_location_hash:
            return self.ip_to_location_hash[ip]
        else:
            response = requests.get(f"https://ipapi.co/{ip}/json/")
            data = response.json()
            city = data["city"]
            region = data["region"]
            country = data["country_name"]
            latitude = data["latitude"]
            longitude = data["longitude"]
            self.ip_to_location_hash[ip] = {
                "city": city,
                "region": region,
                "country": country,
                "latitude": latitude,
                "longitude": longitude,
            }
            return city, region, country, latitude, longitude

    def json_to_redis(self, json_path="tmp_extracted"):
        index = 0
        
        latest_ts = datetime.datetime(2000, 1, 1, 0, 0, 0)
        earliest_ts = datetime.datetime.utcnow()
        num_tracks = 0
        time_played_ms = 0
        countries = []
        
        for file in glob.glob(f"{json_path}/**/*.json", recursive=True):
            with open(file, "r") as f:
                data = json.load(f)
                
            pipe = self.redis.pipeline()

            for track in tqdm.tqdm(data):
                timestamp = track["ts"]
                ts_datetime = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')
                latest_ts = max(latest_ts, ts_datetime)
                earliest_ts = min(earliest_ts, ts_datetime)
                platform = track["platform"]
                ms_played = track["ms_played"]
                time_played_ms += ms_played
                ip = track["ip_addr_decrypted"]
                # ip_city, ip_region, ip_country, ip_latitude, ip_longitude = (
                #     self.ip_to_location(ip)
                # )
                track_name = track["master_metadata_track_name"]
                track_artist = track["master_metadata_album_artist_name"]
                track_album = track["master_metadata_album_album_name"]
                spotify_uri = track["spotify_track_uri"]
                reason_start = track["reason_start"]
                reason_end = track["reason_end"]
                
                try:
                    countries.append(pycountry.countries.get(alpha_2=track["conn_country"]).name)
                except AttributeError:
                    countries.append(track["conn_country"])
                
                try:
                    was_shuffled = int(track["shuffle"])
                    was_skipped = int(track["skipped"])
                    was_offline = int(track["offline"])
                except TypeError:
                    continue
                
                mapping =  {
                            "timestamp": timestamp,
                            "platform": platform,
                            "ms_played": ms_played,
                            # "ip_city": ip_city,
                            # "ip_region": ip_region,
                            # "ip_country": ip_country,
                            # "ip_latitude": ip_latitude,
                            # "ip_longitude": ip_longitude,
                            "track_name": track_name,
                            "track_artist": track_artist,
                            "track_album": track_album,
                            "spotify_uri": spotify_uri,
                            "reason_start": reason_start,
                            "reason_end": reason_end,
                            "was_shuffled": was_shuffled,
                            "was_skipped": was_skipped,
                            "was_offline": was_offline,
                        }
                
                if not all(1 if x is not None else 0 for x in mapping.values()): continue

                try:
                    pipe.hset(
                        f"track:{index}",
                        mapping=mapping,
                    )
                except redis.exceptions.DataError:
                    print(mapping)
                    exit()
                    
                index += 1
            
            pipe.execute()
        
        num_tracks = index
        countries_counter = Counter(countries)
        sorted_countries = countries_counter.most_common()

        print(f"Latest timestamp: {latest_ts}")
        print(f"Earliest timestamp: {earliest_ts}")
        print(f"Number of tracks: {num_tracks}")
        print(f"Time played: {time_played_ms / 1000 / 60 / 60:.2f} hours, or {time_played_ms / 1000 / 60 / 60 / 24:.2f} days straight")
        print(f"On average, that's {time_played_ms / 1000 / 60 / 60 / (latest_ts - earliest_ts).days} per day")
        
        print(f"Countries:")
        for country, count in sorted_countries:
            print(f"    {country}: {count}")            
    

if __name__ == "__main__":
    parser = ZipParser()
    parser.extract_json_from_zip(
        "/home/dion/Downloads/Telegram Desktop/my_spotify_data.zip"
    )
    parser.json_to_redis()
