import os
import time
from lib_pmgrpcd import PMGRPCDLOG
import lib_pmgrpcd
import sys
import ujson as json
from abc import ABC, abstractmethod

sys.path.append("/home/camilo/telemetry/base")
sys.path.append("/home/camilo/telemetry/protos")

jsonmap = {}
avscmap = {}


example_dict = {}


class Exporter(ABC):
    @abstractmethod
    def process_metric(self, metric):
        pass


EXPORTERS = {}


def export_metrics(datajsonstring):
    for exporter in EXPORTERS:
        try:
            EXPORTERS[exporter].process_metric(datajsonstring)
        except Exception as e:
            PMGRPCDLOG.debug("Error processing packet on exporter %s. Error was %s", exporter, e)
            raise


def examples(dictTelemetryData_mod, jsonTelemetryData):
    global example_dict
    if dictTelemetryData_mod["collector"]["grpc"]["grpcPeer"]:
        grpcPeer = dictTelemetryData_mod["collector"]["grpc"]["grpcPeer"]
        if dictTelemetryData_mod["collector"]["grpc"]["ne_vendor"]:
            ne_vendor = dictTelemetryData_mod["collector"]["grpc"]["ne_vendor"]
            if dictTelemetryData_mod["collector"]["data"]["encoding_path"]:
                encoding_path = dictTelemetryData_mod["collector"]["data"][
                    "encoding_path"
                ]

                PMGRPCDLOG.debug(
                    "IN EXAMPLES: grpcPeer=%s ne_vendor=%s encoding_path=%s"
                    % (grpcPeer, ne_vendor, encoding_path)
                )

    try:
        if not os.path.exists(lib_pmgrpcd.OPTIONS.examplepath):
            os.makedirs(lib_pmgrpcd.OPTIONS.examplepath)
    except OSError:
        pass
    if grpcPeer not in example_dict:
        example_dict.update({grpcPeer: []})

    if encoding_path not in example_dict[grpcPeer]:
        example_dict[grpcPeer].append(encoding_path)
        encoding_path_mod = encoding_path.replace(":", "_").replace("/", "-")

        exafilename = grpcPeer + "_" + ne_vendor + "_" + encoding_path_mod + ".json"
        exapathfile = os.path.join(lib_pmgrpcd.OPTIONS.examplepath, exafilename)

        with open(exapathfile, "w") as exapathfile:
            # exapathfile.write("PROTOPATH[" + telemetry_node + "]: " + protopath + "\n")
            exapathfile.write(jsonTelemetryData)
            exapathfile.write("\n")


def FinalizeTelemetryData(dictTelemetryData):

    # Adding epoch in millisecond to identify this singel metric on the way to the storage
    epochmillis = int(round(time.time() * 1000))
    dictTelemetryData["collector"]["data"].update({"collection_timestamp": epochmillis})

    dictTelemetryData_mod = dictTelemetryData.copy()

    # Going over the mitigation library, if needed.
    # TODO: Simplify the next part
    if lib_pmgrpcd.OPTIONS.mitigation:
        from mitigation import mod_all_json_data
        try:
            dictTelemetryData_mod = mod_all_json_data(dictTelemetryData_mod)
            jsonTelemetryData = json.dumps(
                dictTelemetryData_mod, indent=2, sort_keys=True
            )
        except Exception as e:
            PMGRPCDLOG.info("ERROR: mod_all_json_data raised a error:\n%s")
            PMGRPCDLOG.info("ERROR: %s" % (e))
            dictTelemetryData_mod = dictTelemetryData
            jsonTelemetryData = json.dumps(dictTelemetryData, indent=2, sort_keys=True)
    else:
        dictTelemetryData_mod = dictTelemetryData
        jsonTelemetryData = json.dumps(dictTelemetryData, indent=2, sort_keys=True)

    PMGRPCDLOG.debug("After mitigation: %s" % (jsonTelemetryData))

    if lib_pmgrpcd.OPTIONS.examplepath and lib_pmgrpcd.OPTIONS.example:
        examples(dictTelemetryData_mod, jsonTelemetryData)

    if lib_pmgrpcd.OPTIONS.jsondatafile:
        PMGRPCDLOG.debug("Write jsondatafile: %s" % (lib_pmgrpcd.OPTIONS.jsondatafile))
        with open(lib_pmgrpcd.OPTIONS.jsondatafile, "a") as jsondatafile:
            jsondatafile.write(jsonTelemetryData)
            jsondatafile.write("\n")

    # Filter only config.
    export = True
    if lib_pmgrpcd.OPTIONS.onlyopenconfig:
        PMGRPCDLOG.debug(
            "only openconfig filter matched because of options.onlyopenconfig: %s"
            % lib_pmgrpcd.OPTIONS.onlyopenconfig
        )
        export = False
        if "encoding_path" in dictTelemetryData_mod["collector"]["data"]:
            if (
                "openconfig"
                in dictTelemetryData_mod["collector"]["data"]["encoding_path"]
            ):
                export = True

    if export:
        export_metrics(jsonTelemetryData)

    return jsonTelemetryData
