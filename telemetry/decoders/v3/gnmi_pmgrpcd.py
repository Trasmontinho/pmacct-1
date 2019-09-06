"""
Implements a simple gNMI client. Still no fancy features here, like:
    - Evaluting duplicates to detect slow consumption
    - Capabilites. 
    - Related to Capabilities: etecting if the paths are supported by target (since it seems that some targets simply do not send anything and do not complain about an unsupported path)
The specifications for gnmi can be found in https://github.com/openconfig/gnmi
Although the gnmi standard is quite detailed, it was very nice to see python examples of the interface from https://github.com/nokia/pygnmi
"""
import gnmi_pb2
import gnmi_pb2_grpc
import lib_pmgrpcd
import gnmi_utils
from lib_pmgrpcd import PMGRPCDLOG
from export_pmgrpcd import FinalizeTelemetryData



class GNMIClient:

    def __init__(self, channel):
        self.channel = channel
        self.stub = gnmi_pb2_grpc.gNMIStub(self.channel)
        # ask for the capabilites
        #cap_req = gnmi_pb2.CapabilityRequest()
        #cap_res = self.stub.Capabilities(cap_req)
        self.encapsulation = gnmi_pb2.PROTO
        encoding_path = "/interfaces"
        path = gnmi_utils.simple_gnmi_string_parser(encoding_path)
        mysub = gnmi_pb2.Subscription(path=path, sample_interval=60*1000000000)
        mysubs = [mysub]
        mysblist = gnmi_pb2.SubscriptionList(prefix=None, encoding=self.encapsulation, subscription=mysubs)
        mysubreq = gnmi_pb2.SubscribeRequest( subscribe=mysblist )
        def x():
            yield mysubreq
        y = x()
        base_grpc = {"grpcPeer": self.channel._channel.target().decode(), "ne_vendor": "gnmi"}

        msgs  = self.stub.Subscribe(y, None)
        for msg in msgs:
            if msg.HasField('update'):
                grpc = dict(base_grpc)
                data = {"node_id_str": "r33.labxtx01.us.bb"}
                notification = msg.update
                timestamp = notification.timestamp # in nanoseconds since epoch
                prefix = notification.prefix
                sensor_path, keys  = gnmi_utils.gnmi_to_string_and_keys(prefix)
                data["encoding_path"] = sensor_path
                data["collection_timestamp"] = timestamp / 1000
                data["keys"] = keys
                gnmi = []
                header_info = None
                for upd in notification.update:
                    upd_name, extra_keys  = gnmi_utils.gnmi_to_string_and_keys(upd.path)
                    try:
                        value = getattr(upd.val, upd.val.WhichOneof("value"))
                    except:
                        breakpoint()
                    if upd.val.WhichOneof("value") in ("leaflist_val", "any_val", "decimal_val"):
                        value = str(value)
                    if upd_name == "__juniper_telemetry_header__":
                        header_bytes = value
                        continue

                    if extra_keys:
                        breakpoint()
                    gnmi.append({"keys": extra_keys, "name": upd_name, "value": value})
                data["gnmi"] = gnmi
                message_dict = {"collector": {"grpc": grpc, "data": data}}

                try:
                    returned = FinalizeTelemetryData(message_dict)
                except Exception as e:
                    PMGRPCDLOG.error("Error finalazing  message: %s", e)


                    

                    





