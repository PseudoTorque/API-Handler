from multiprocessing import Process
from multiprocessing.managers import SyncManager
from socket import socket, AF_INET, SOCK_STREAM
import time
from utils import encode_object, decode_object


class man(SyncManager):
    pass

def main(conn: socket) -> None:

    while True:

        time.sleep(1)
        print(decode_object(conn["conn"]))

def main2(conn: socket) -> None:

    time.sleep(5)
    conn["conn"].close()

    print("Closed")

if __name__ == "__main__":
    host = "localhost"
    port = 8080

    sock = socket(AF_INET, SOCK_STREAM)

    sock.bind((host, port))

    sock.listen()
    conne, _ = sock.accept()

    manager = man()
    manager.start()

    conn = manager.dict()

    conn["conn"] = conne

    
    

    p1 = Process(target=main, args=(conn,))
    p1.start()

    p2 = Process(target=main2, args=(conn,))
    p2.start()

    p1.join()
    p2.join()

    manager.join()
