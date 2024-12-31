from utils import encode_object, decode_object
import time

from socket import socket, AF_INET, SOCK_STREAM

from objects import Details, Dummy, Priority, Close, SwitchToBatch, SwitchToStream, Rename

import random, utils



if __name__ == "__main__":

    socket = socket(AF_INET, SOCK_STREAM)

    socket.connect(("localhost", 8080))

    while True:

        c = input("Enter an object: [close to close connection]")

        if c == "close":
            socket.send(encode_object(Close()))
            socket.close()
            break
        if c == "batch":
            socket.send(encode_object(SwitchToBatch(15)))
            continue
        if c == "stream":
            socket.send(encode_object(SwitchToStream()))
            continue
        if "rename" in c:
            socket.send(encode_object(Rename(c.split(" ")[1])))
            continue

        if "freq" in c:
            
            while utils.cycle(float(c.split(" ")[1])):
                p = Priority(random.randint(0, 9), random.randint(0, 9))
                socket.send(encode_object(Dummy(c, p)))

                if socket.getblocking():
                    socket.setblocking(False)
                decoded = decode_object(socket)
        else:

            c = int(c.split(" ")[1])

            for i in range(c):

                p = Priority(random.randint(0, 9), random.randint(0, 9))

                socket.send(encode_object(Dummy(c, p)))

            count = 0
            while count < c:
                time.sleep(0.1)
                result = decode_object(socket)
                
                if isinstance(result, Details):
                    result = result.state, result.reason, result.instruction
                else:
                    count += 1
                print(str(result) + " t: " + str(time.time()))



