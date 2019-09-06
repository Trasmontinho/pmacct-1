import time
from export_pmgrpcd import export_metrics

class FileInput():
    def __init__(self, filename, max_metrics_per_packet = None, time_between_packets=None):
        self.filename = filename
        self.max_metrics_per_packet = max_metrics_per_packet
        self.time_between_packets = time_between_packets

    def generate(self):
        with open(self.filename, "r") as fh:
            for line in fh:
                export_metrics(line)
                if self.time_between_packets:
                    time.sleep(self.time_between_packets)


