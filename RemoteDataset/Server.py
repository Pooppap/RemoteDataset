import os
import pynng
import msgpack
import argparse
import numpy as np
import msgpack_numpy

from glob import glob
from time import sleep


msgpack_numpy.patch()

parser = argparse.ArgumentParser()
parser.add_argument(
    '--main_path',
    type=str,
    help='Main path'
)
parser.add_argument(
    '--glob_pattern',
    type=str,
    help='Glob pattern'
)
parser.add_argument(
    '--port',
    type=int,
    help='Port'
)
parser.add_argument(
    '--debug',
    type=int,
    help='Debug'
)


def main(args):
    print("Looking for files...")
    file_list = glob(os.path.join(args.main_path, args.glob_pattern))
    
    dataset = {}
    for file in file_list:
        subj, *date = file.split(os.sep)[-1].split("_")
        date = "_".join(date)
        subj_dates_list = dataset.get(subj, [])
        if date in subj_dates_list:
            raise ValueError("Duplicate entries found")

        subj_dates_list.append(date)
        dataset[subj] = subj_dates_list
        
    for subj, dates in dataset.items():
        dates.sort()
        if len(dates) > 15:
            mid_point = len(dates) // 2
            dataset[subj] = dates[:5] + dates[mid_point - 2:mid_point + 3] + dates[-5:]
        
    print("Starting server...")
    reply_server = pynng.Rep0()
    reply_server.listen(f"tcp://127.0.0.1:{args.port}")
    
    while True:
        request = reply_server.recv().decode().split(":")
        if request[0] != "r":
            raise ValueError("Invalid request header")
        
        if request[1] == "start":
            if args.debug:
                print("Sending dataset list")
            reply = msgpack.packb(dataset)
            reply_server.send(reply)
        
        elif request[1] == "dataset":
            if args.debug:
                print(f"Sending dataset {request[2]}")

            try:
                data = np.load(os.path.join(args.main_path, request[2]))["resampled_matrices"]
            except:
                reply = b"-1"
                reply_server.send(reply)
                continue

            data = np.hstack(
                [
                    data[0, :, 4:7],
                    data[0, :, 1:4],
                    # data[:, 3:6],

                    data[1, :, 4:7],
                    data[1, :, 1:4],
                    # data[:, 12:15],

                    data[2, :, 4:7],
                    data[2, :, 1:4],
                    # data[:, 21:24],

                    data[3, :, 4:7],
                    data[3, :, 1:4],
                    # data[:, 30:33],
                ]
            )
            
            data_slice_start = np.arange(0, data.shape[0] - 80 + 1, 80).astype(np.int64)
            reply = f"{len(data_slice_start)}".encode()
            reply_server.send(reply)
        
        elif request[1] == "index":
            if args.debug:
                print(f"Sending index {int(request[2])}")

            starting_index = int(request[2])
            data_chunk = data[starting_index:starting_index + 80]
            reply = msgpack.packb(data_chunk)
            reply_server.send(reply)
        
        elif request[1] == "whole":
            data_whole = np.stack([data[starting_index:starting_index + 80, :] for starting_index in data_slice_start])
            reply = msgpack.packb(data_whole)
            reply_server.send(reply)
        
        elif request[1] == "close":
            print("Closing server...")
            sleep(1)
            reply_server.close()
            break
        
        else:
            raise ValueError("Invalid request type")
        
if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
