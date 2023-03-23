import csv
from faker import Faker

HEADER = ["Name", "Address", "City", "State", "Zipcode"]


def generate_fake_csv(num_rows: int = 20,
                      header: list = HEADER,
                      save_path: str = "../datasets/rdd/sample2.csv"):
    fake = Faker()
    data = [[fake.name(), fake.address(), fake.city(), fake.state_abbr(), fake.zipcode()]
            for _ in range(num_rows)]

    if save_path:
        with open(save_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)
            writer.writerows(data)


generate_fake_csv()