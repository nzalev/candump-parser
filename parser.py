from pymongo import MongoClient


def parse_line(line: str) -> tuple:
    parts = line.split()
    a_id: str = parts[1]
    byte_len: int = int(parts[2][1])
    bytes_str: str = ' '.join(parts[3:])

    return (a_id, byte_len, bytes_str)


def insert_line(coll, line: tuple) -> None:
    message = {
        'arbitration_id': line[0],
        'data_len': line[1],
        'data_string': line[2]
    }

    coll.insert_one(message)


def process_file(filename: str, collection) -> None:
    with open(filename) as f:
        data = f.readlines()

    for line in data:
        l = line.strip('\n')
        insert_line(collection, parse_line(l))



client = MongoClient('mongodb://127.0.0.1:27017')
db = client.canmsgs
can_a = db.can_a
can_b = db.can_b

process_file('cana.txt', can_a)
process_file('canb.txt', can_b)


