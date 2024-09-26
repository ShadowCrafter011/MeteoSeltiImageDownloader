from requests.structures import CaseInsensitiveDict
from multiprocessing import Process, Queue
from shadowbar import ProgressBar
from dotenv import load_dotenv
from UrlCreator import api
import requests
import json
import os


def download(queue, progress):
    while queue.qsize() > 0:
        timestamp, url = queue.get()

        try:
            image_bytes = requests.get(url).content

            with open(f"./data/{timestamp}.jpg", "wb") as image:
                image.write(image_bytes)
        except requests.exceptions.ChunkedEncodingError:
            pass

        progress.value += 1


def main():
    load_dotenv()
    headers = CaseInsensitiveDict({
        "Authorization": os.getenv("BEARER_TOKEN")
    })

    count = requests.get(api("measurement/count"), headers=headers).json()
    frames = count["frames"]

    images = Queue()

    print("Fetching image URLs...")
    pbar_progress, pbar = ProgressBar.new(50, frames)

    with open("./cloud_status.json") as f:
        cloud_status = json.loads(f.read())


    for frame in range(frames):
        imgs = requests.get(api(f"retrieve/images/{frame}"), headers=headers).json()["data"]
        for _, (id, data) in enumerate(imgs.items()):
            image_path = f"./data/{data['timestamp']}.jpg"

            cloud_status[id] = { "has_cloud_status": data["has_cloud_status"], "image_path": image_path }

            if os.path.exists(image_path):
                continue

            images.put((data["timestamp"], data["url"]))
        pbar_progress.value += 1
    pbar.wait_complete()

    with open("./cloud_status.json", "w") as f:
        f.write(json.dumps(cloud_status))

    print("Downloading images...")
    pbar_progress, pbar = ProgressBar.new(50, images.qsize())

    download_processes = []
    for _ in range(16):
        process = Process(target=download, args=(images, pbar_progress))
        download_processes.append(process)
        process.start()

    for process in download_processes:
        process.join()

    pbar.wait_complete()


if __name__ == '__main__':
    main()
