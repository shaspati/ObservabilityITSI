import json

# list of  metrics we start now
metricsTypes = ["vmdstat", "vmiostat", "dstat", "iostat", "mds", "ucs"]

logType2CINameMapping = {
    "vmdstat": "vmname",
    "vmiostat": "vmname",
    "dstat": "hostname",
    "iostat": "hostname",
    "mds": "mds",
    "ucs": "ucs",
}

# list of CI names we care for now
ci_list = {}
with open("ci_list.json") as f:
    ci_list = json.load(f)

# open file metricsLogs.log to write logs
logFile = open("metricsLogs.log", "a")
