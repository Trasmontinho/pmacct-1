import export_pmgrpcd
import lib_pmgrpcd
from zmq_modules.zmq_exporter import ZmqExporter
from kafka_modules.kafka_avro_exporter import KafkaAvroExporter, manually_serialize
from kafka_modules.kafka_simple_exporter import KafkaExporter
from file_modules.file_producer import FileExporter
from lib_pmgrpcd import PMGRPCDLOG

def configure(config=None):
    if config is None:
        config = lib_pmgrpcd.OPTIONS

    # Add the exporters

    if config.zmq:
        zmq_exporter = ZmqExporter()
        export_pmgrpcd.EXPORTERS["zmq"] = zmq_exporter
    if config.kafkaavro:
        kafka_avro_exporter = KafkaAvroExporter()
        export_pmgrpcd.EXPORTERS["kafkaavro"] = kafka_avro_exporter
    if config.kafkasimple:
        exporter = KafkaExporter(config.bsservers, config.topic)
        export_pmgrpcd.EXPORTERS["kafka"] = exporter
    if config.file_exporter_file is not None:
        exporter = FileExporter(config.file_exporter_file)
        export_pmgrpcd.EXPORTERS["file"] = exporter

