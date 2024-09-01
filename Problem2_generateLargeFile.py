import csv
import os
from faker import Faker

# Initialize Faker
fake = Faker()

# Target file size in bytes (2GB)
target_size = 2 * 1024 * 1024 * 1024  # 2GB in bytes

# File name for the output CSV
file_name = 'large_file.csv'

# Write to the CSV file
with open(file_name, 'w', newline='') as csvfile:
    fieldnames = ['first_name', 'last_name', 'email', 'date_of_birth']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()

    # Keep track of the file size
    current_size = os.path.getsize(file_name)

    # Generate data until the file size reaches the target
    while current_size < target_size:
        # Generate a fake row
        row = {
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email().replace("\n", " "),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90)
        }
        # Write the row to the file
        writer.writerow(row)

        # Update the current file size
        current_size = os.path.getsize(file_name)

print(f"{file_name} has been created with a size of approximately {target_size / (1024 * 1024)} MB.")
