import time
import uuid
import random
import os

from datetime import date

from faker import Faker


# Specify the directory to save the files
today = date.today().strftime("%Y-%m-%d")
directory = f"files_to_ingest/day={today}"
os.makedirs(directory, exist_ok=True)

fake = Faker()

count = 0

while True:
    # Generate a unique filename using UUID
    prefix = str(count).zfill(4)
    filename = f"{prefix}-{uuid.uuid4()}.txt"
    filepath = os.path.join(directory, filename)

    # Create and write to the file
    with open(filepath, "w") as file:
        word = fake.word()
        file.write(word)

    count += 1

    # Wait for a random time between 10 and 20 seconds
    time.sleep(random.randint(30, 60))
