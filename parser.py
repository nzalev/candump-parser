from pymongo import MongoClient


def parse_line(line: str) -> tuple:
    parts = line.split()
    a_id: str = parts[1]
    byte_len: int = int(parts[2][1])
    bytes_str: str = ' '.join(parts[3:])

    return (a_id, byte_len, bytes_str)


def insert_line(line: tuple, counter: int, frames: list) -> None:
    message = {
        'arbitration_id': line[0],
        'data_len': line[1],
        'data_string': line[2],
        'counter': counter
    }

    frames.append(message)


def process_file(filename: str, collection) -> None:
    with open(filename) as f:
        data = f.readlines()

    frames = []
    counter = 0
    for line in data:
        l = line.strip('\n')
        insert_line(parse_line(l), counter, frames)
        counter += 1

    collection.insert_many(frames)



client = MongoClient('mongodb://127.0.0.1:27017')
db = client.canmsgs
can_a = db.idlebattery


process_file('1731.txt', can_a)



