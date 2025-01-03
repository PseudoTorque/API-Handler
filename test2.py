from socket import socket, AF_INET, SOCK_STREAM
import time

from utils import encode_object, decode_object

sock = socket(AF_INET, SOCK_STREAM)

sock.connect(("localhost", 8080))

while True:

    sock.send(encode_object("Hello, world!"))

    time.sleep(1)

sock.close()
