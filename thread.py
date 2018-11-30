import time
from threading import Thread

i = 0
class PrintThread(Thread):
    def run(self):
        print("Hello World")
        global i
        for _ in range(1000):
            j = i
            time.sleep(.0001)
            i = j + 1

t = PrintThread()
t2 = PrintThread()
t.start()
t2.start()
t.join()
t2.join()
print(i)
