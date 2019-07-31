import Deepchat as dc
import io
if __name__ == "__main__":
    dc.CreateTable
    with open('Data/2017-10.db', mode='w', buffering=1000) as f:
        for i in range(100000):
            f.write('teste???')
        print(io.DEFAULT_BUFFER_SIZE)
        f.close()
