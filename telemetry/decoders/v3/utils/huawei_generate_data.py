from huawei_generator  import HuaweiDialOutClient
from  huawei_grpc_dialout_pb2  import serviceArgs
from utils import generate_content_from_raw
from optparse import OptionParser

DEFAULT_FILE = "huawei_dump"
DEFAULT_CONNECTION = "127.0.0.1:6000"

parser = OptionParser()
parser.add_option(
    "-f",
    "--file",
    default=str(DEFAULT_FILE),
    dest="file",
    help="File with raw data",
)
parser.add_option(
    "-c",
    "--connection",
    default=str(DEFAULT_CONNECTION),
    help="IP (socket address) of the collector",
)

(options, _) = parser.parse_args()


huawei_client = HuaweiDialOutClient(options.connection)
def generate_data(data_generator):
    for data in data_generator:
        yield serviceArgs(ReqId=1, data=data)

huawei_client.send_data(generate_data(generate_content_from_raw(options.file)))

# check status
while not huawei_client.rcv.done():
    continue
try:
    result = huawei_client.rcv.result()
except Exception as e:
    print("Generation failed with error ", e)





