
from zmq import ZMQError
import zmq
import lib_pmgrpcd
from export_pmgrpcd import Exporter

class ZmqExporter(Exporter):
    def __init__(self):
        zmqContext = zmq.Context()
        self.zmqSock = zmqContext.socket(zmq.PUSH)
        self.zmqSock.bind(lib_pmgrpcd.OPTIONS.zmqipport)
        self.flags = zmq.NOBLOCK

    def process_metric(self, datajsonstring):
        if not self.zmqSock.closed:
            try:
                self.zmqSock.send_json("%s" % datajsonstring, self.flags)
            except ZMQError:
                lib_pmgrpcd.SERIALIZELOG.debug(
                    "ZMQError: %s" % (lib_pmgrpcd.OPTIONS.jsondatafile)
                )
