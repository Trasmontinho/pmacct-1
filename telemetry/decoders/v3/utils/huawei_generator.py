# Imitates the generation of huawei telemetry.
# Use for testing.
import huawei_grpc_dialout_pb2_grpc
import grpc

# Dialout code
# Simple for now, it could get complicated if we need a more time based example.
class HuaweiDialOutClient():
    def __init__(self, server):
        self.server = server
        self.channel = grpc.insecure_channel(self.server)
        self.stub = huawei_grpc_dialout_pb2_grpc.gRPCDataserviceStub(self.channel)

    def send_data(self, data):
        self.rcv  = self.stub.dataPublish(data)


    def close(self):
        self.channel.close()


