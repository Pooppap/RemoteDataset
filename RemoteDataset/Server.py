import os
import socket
import msgpack
import argparse
import numpy as np
import msgpack_numpy

from glob import glob


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


def main(args):
    
