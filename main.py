from utils import encode_object

from socket import socket, AF_INET, SOCK_STREAM

socket = socket(AF_INET, SOCK_STREAM)

socket.connect(("localhost", 8080))

while True:

    c = input("Enter an object: ")

    socket.send(encode_object(c))


