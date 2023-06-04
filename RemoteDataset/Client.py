import torch
import pynng
import msgpack
import sshtunnel
import msgpack_numpy


msgpack_numpy.patch()


class RemoteDataAdapter(torch.utils.data.Dataset):
    def __init__(self, dataset_length, stat, port) -> None:
        self.stat = stat
        self.request_server = pynng.Req0()
        self.dataset_length = dataset_length
        self.request_server.dial(f"tcp://127.0.0.1:{port}")
        
    def __len__(self):
        return self.dataset_length
    
    def __getitem__(self, idx):
        request = f"r:index:{idx}"
        self.request_server.send(request.encode())
        packed_data = self.request_server.recv()
        data = msgpack.unpackb(packed_data)
        
        data = torch.from_numpy(data)
        data = self.to_norm(data, self.stat[0], self.stat[1]).t()
        
        return data
        
    @staticmethod
    def to_norm(data, means, stds):
        norm = (data - means) / stds
        return norm.float()



class RemoteDataset:
    def __init__(self, ssh_config, port, stat) -> None:
        self.stat = stat
        self.port = port
        self.tun = sshtunnel.SSHTunnelForwarder(
            "apollo.doc.ic.ac.uk",
            ssh_username=ssh_config["SSH_USERNAME"],
            ssh_password=ssh_config["SSH_PASSWORD"],
            ssh_port=10022,
            remote_bind_address=('127.0.0.1', port),
            local_bind_address=("", port)
        )
        self.request_server = pynng.Req0()
        
    def connect(self):
        self.tun.start()
        self.request_server.dial(f"tcp://127.0.0.1:{self.port}")
        
    def __len__(self):
        self.request_server.send(b"r:start")
        datasets = self.request_server.recv()
        self.datasets = msgpack.unpackb(datasets)
        return len(self.datasets)
    
    def __iter__(self):
        for subj, dates in self.datasets.items():
            for date in dates:
                request = "_".join([subj, date])
                self.request_server.send(b"r:dataset:" + request.encode())
                dataset_length = int(self.request_server.recv())
                yield subj, date.split("_")[0], RemoteDataAdapter(dataset_length, self.stat, self.port)
                
    def close(self):
        self.request_server.send(b"r:close")
        self.request_server.close()
        self.tun.stop()
