from __future__ import print_function
from zmq import ZMQError
import zmq
from optparse import OptionParser
import sys

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)



DEFAULT_PORT = "tcp://127.0.0.1:50000"

parser = OptionParser()
parser.add_option(
    "-p",
    "--port",
    default=str(DEFAULT_PORT),
    dest="port",
    help="Port to setup the server",
)
(options, _) = parser.parse_args()

zmqContext = zmq.Context()
zmqSock = zmqContext.socket(zmq.PULL)
zmqSock.connect(options.port)
eprint("zmq ready")

while True:
    work = zmqSock.recv_json().strip()
    if work:
        print(work)
