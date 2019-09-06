from export_pmgrpcd import Exporter
import os
import ujson as json

class FileExporter(Exporter):
    def __init__(self, output_file):
        self.output_file = output_file

    def process_metric(self, datajsonstring):
        jsondata = json.loads(datajsonstring)
        again_json = json.dumps(jsondata).replace("\n", "")
        # we'll probably remove the loading here
        with open(self.output_file, 'a') as fh:
            fh.write(again_json)
            fh.write("\n")

