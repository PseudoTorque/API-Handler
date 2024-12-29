from utils import encode_object, decode_object
import time

from socket import socket, AF_INET, SOCK_STREAM

from constants import Dummy, Priority, Signal, Signals, SignalPriorities

import random



if __name__ == "__main__":

    socket = socket(AF_INET, SOCK_STREAM)

    socket.connect(("localhost", 8080))

    while True:

        c = input("Enter an object: [close to close connection]")

        if c == "close":
            socket.send(encode_object(Signal(Signals.close, SignalPriorities.close)))
            socket.close()
            break

        sent=[]
        for i in range(10):
            p = Priority(random.randint(0, 9), random.randint(0, 9))
            sent.append((p.group_priority, p.internal_priority, time.time()))
            socket.send(encode_object(Dummy(c, p)))

        for i in range(10):
            print(decode_object(socket) + " t: " + str(time.time()))



