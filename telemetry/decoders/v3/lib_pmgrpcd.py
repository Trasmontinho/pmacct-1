import logging
from pathlib import Path

SCRIPTVERSION = "1.1"

class FileNotFound(Exception):
    pass


PMGRPCDLOG = logging.getLogger("PMGRPCDLOG")
OPTIONS = None
MISSGPBLIB = {}


def init_pmgrpcdlog():
    global PMGRPCDLOG, OPTIONS
    PMGRPCDLOG.setLevel(logging.DEBUG)
    grformatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # create file handler which logs even debug messages
    grfh = logging.FileHandler(OPTIONS.PMGRPCDLOGfile)
    if OPTIONS.debug:
        grfh.setLevel(logging.DEBUG)
    else:
        grfh.setLevel(logging.INFO)

    grfh.setFormatter(grformatter)
    PMGRPCDLOG.addHandler(grfh)

    if OPTIONS.console:
        # create console handler with a higher log level
        grch = logging.StreamHandler()
        if OPTIONS.debug:
            grch.setLevel(logging.DEBUG)
        else:
            grch.setLevel(logging.INFO)

        grch.setFormatter(grformatter)
        PMGRPCDLOG.addHandler(grch)


def init_serializelog():
    global SERIALIZELOG
    SERIALIZELOG = logging.getLogger("SERIALIZELOG")
    SERIALIZELOG.setLevel(logging.DEBUG)
    seformatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # create file handler which logs even debug messages
    sefh = logging.FileHandler(OPTIONS.serializelogfile)
    if OPTIONS.debug:
        sefh.setLevel(logging.DEBUG)
    else:
        sefh.setLevel(logging.INFO)

    sefh.setFormatter(seformatter)
    SERIALIZELOG.addHandler(sefh)

    if OPTIONS.console:
        # create console handler with a higher log level
        sech = logging.StreamHandler()
        if OPTIONS.debug:
            sech.setLevel(logging.DEBUG)
        else:
            sech.setLevel(logging.INFO)

        sech.setFormatter(seformatter)
        SERIALIZELOG.addHandler(sech)


def signalhandler(signum, frame):
    global MISSGPBLIB
    # pkill -USR1 -e -f "python.*pmgrpc"
    if signum == 10:
        PMGRPCDLOG.info("Signal handler called with USR1 signal: %s" % (signum))
        PMGRPCDLOG.info("These are the missing gpb libs: %s" % (MISSGPBLIB))
    if signum == 12:
        PMGRPCDLOG.info("Signal handler called with USR2 signal: %s" % (signum))
        PMGRPCDLOG.info("TODO: %s" % ("todo"))
