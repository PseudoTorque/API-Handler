from utils import encode_object, decode_object
import time

from objects import Details, Dummy, Priority, Close, SwitchToBatch, SwitchToStream, Rename

import random, utils
from interface import Client


if __name__ == "__main__":

    client = Client("localhost", 8080, "test")

    while True:

        c = input("Enter an object: [close to close connection]")

        if c == "close":
            client.close()
            break
        if "rename" in c:
            client.rename(c.split(" ")[1])
            continue

        if "freq" in c:
            
            while utils.cycle(float(c.split(" ")[1])):
                p = Priority(random.randint(0, 9), random.randint(0, 9))
                #print(client.process_stream(Dummy(c, p)))

        else:
            
            batch = []

            c = int(c.split(" ")[1])

            for i in range(c):

                p = Priority(random.randint(0, 9), random.randint(0, 9))
                batch.append(Dummy(c, p))

            print("order sent", [i.priority.__dict__ for i in batch])
            for i in client.process_batch_and_yield_responses(batch, 15):
                print(i)



