#!/usr/bin/env python3.7
#
from datetime import datetime
import pprint
global mitigation
mitigation = {}

def mod_all_json_data(resdict):
  global mitigation
  mitigation = resdict.copy()

  if "collector" in mitigation:
    if ("grpc" in mitigation["collector"]) and ("data" in mitigation["collector"]):
      if "ne_vendor" in mitigation["collector"]["grpc"]:
        mod_all_pre()
        if mitigation["collector"]["grpc"]["ne_vendor"] == "Huawei":
          mod_huawei()
        elif mitigation["collector"]["grpc"]["ne_vendor"] == "Cisco":
          mod_cisco()
        mod_all_post()
  return mitigation

def mod_all_pre():
  global mitigation

  #especially Cisco oc_int has to have a Array after the "subinterface" but anyway it is good for all vendors in case it will be without array
  addanywayarray_subint()

  #Cisco oc_int has to have a Array after the "interface" but anyway it is good for all vendors in case it will be without array
  addanywayarray_int()

  #CHANGE ON EACH KEY THE DASH TO UNDERSCORE because avro only support underscore
  obj = mitigation.copy()
  mitigation = d2u(obj)

def mod_huawei():
  global mitigation
  #rename Huawei sensor_path to encoding_path because it has to match with the avroschema
  sens2enco()

  #"interface":[
  # The parent  container "interfaces" is missing for "interface" in case of Huawei
  #https://github.com/openconfig/public/blob/master/release/models/interfaces/openconfig-interfaces.yang#L1012
  insert_rec_interfaces()

  #"admin_status":0,
  #"oper_status":0,
  #Huawei oc_interface "admin_status" and "oper_status" contains 0 or 1 but has to have "UP" or "DOWN"
  #https://github.com/openconfig/public/blob/master/release/models/interfaces/openconfig-interfaces.yang#L464
  #https://github.com/openconfig/public/blob/master/release/models/interfaces/openconfig-interfaces.yang#L496
  #because Huawei told us that the the hole section state is not in "acquisition path" of openconfig-interfaces i will remove all the leaves of "state".
  #only the section "counters" within the section "state" will be there
  remove_hua_state()

  #"in_unicast_pkts":"595769",
  #Huawei oc_interface ALL counters are strings but should be int
  #https://github.com/openconfig/public/blob/master/release/models/interfaces/openconfig-interfaces.yang#L615
  #"last_clear":"",
  #last_clear has to be epoch (integer) not string
  #https://github.com/openconfig/public/blob/master/release/models/interfaces/openconfig-interfaces.yang#L854
  #https://tools.ietf.org/html/rfc6991
  #typedef timeticks {
  #  type uint32;
  #    description
  #    "The timeticks type represents a non-negative integer that
  #     represents the time, modulo 2^32 (4294967296 decimal), in
  int_state_counters_2_int()

  #encoding_path of Hua has to be mach the jsondata: 
  #"encoding_path":"openconfig-interfaces:interfaces\/interface\/state\/counters" -> "encoding_path":"openconfig-interfaces:interfaces",
  correct_huaw_enc_path()

def mod_cisco():
  cisco_lastclear2epoch()

def mod_all_post():
  global mitigation

  #All collector meta-data part of collector -> data have to be all the time the correct data-type. 
  #independent of the vendor or routertyp. 
  #Only defined keys are allowed because of mandatory avroschema
  #In general remove all kv-pairs of record["collector"]["data"] except this ones:
      #"collection_end_time":1548319798771,
      #"collection_id":"3007",
      #"collection_start_time":1548319798741,
      #"encoding_path":"openconfig-interfaces:interfaces\/interface\/state\/counters",
      #"msg_timestamp":1548319798831,
      #"node_id_str":"ipi-zbb900-r-al-01",
      #"subscription_id_str":"DAISY63"
  harmonize_collector_data()
  

#Helper-Methods for ALL
#---------------------------
def addanywayarray_subint():
  if "encoding_path" in mitigation["collector"]["data"]:
    if mitigation["collector"]["data"]["encoding_path"] == "openconfig-interfaces:interfaces":
      if "interfaces" in mitigation:
        if "interface" in mitigation["interfaces"]:
          if "subinterfaces" in mitigation["interfaces"]["interface"]:
            if "subinterface" in mitigation["interfaces"]["interface"]["subinterfaces"]:
              if type(mitigation["interfaces"]["interface"]["subinterfaces"]["subinterface"]) == dict:
                mitigation["interfaces"]["interface"]["subinterfaces"]["subinterface"] = [mitigation["interfaces"]["interface"]["subinterfaces"]["subinterface"]]

def addanywayarray_int():
  if "encoding_path" in mitigation["collector"]["data"]:
    if mitigation["collector"]["data"]["encoding_path"] == "openconfig-interfaces:interfaces":
      if "interfaces" in mitigation:
        if "interface" in mitigation["interfaces"]:
          if type(mitigation["interfaces"]["interface"]) == dict:
            mitigation["interfaces"]["interface"] = [mitigation["interfaces"]["interface"]]

def d2u(obj):
  if isinstance(obj, (str, int, float)):
      return obj
  if isinstance(obj, dict):
      new = obj.__class__()
      for k, v in obj.items():
          new[k.replace('-', '_')] = d2u(v)
  elif isinstance(obj, (list, set, tuple)):
      new = obj.__class__(d2u(v) for v in obj)
  else:
      return obj
  return new

def harmonize_collector_data():
  coldata = mitigation["collector"]["data"].copy()
  newcoldata = {}

  inttype = ["collection_timestamp", "collection_end_time", "collection_start_time", "msg_timestamp"]
  stringtype = ["collection_id", "encoding_path", "node_id_str", "subscription_id_str", "encoding_type"]

  for elem in mitigation["collector"]["data"]:
    if elem in inttype:
      newcoldata.update({elem: int(mitigation["collector"]["data"][elem])})
      inttype.remove(elem)
    elif elem in stringtype:
      newcoldata.update({elem: str(mitigation["collector"]["data"][elem])})
      stringtype.remove(elem)
    else:
      pass
  for elem in inttype:
    newcoldata.update({elem: -1})
  for elem in stringtype:
    newcoldata.update({elem: "None"})

#  newcoldata.update({"collection_timestamp": int(mitigation["collector"]["data"]["collection_timestamp"])})
#  newcoldata.update({"collection_end_time": int(mitigation["collector"]["data"]["collection_end_time"])})
#  newcoldata.update({"collection_id": str(mitigation["collector"]["data"]["collection_id"])})
#  newcoldata.update({"collection_start_time": int(mitigation["collector"]["data"]["collection_start_time"])})
#  newcoldata.update({"encoding_path": str(mitigation["collector"]["data"]["encoding_path"])})
#  newcoldata.update({"msg_timestamp": int(mitigation["collector"]["data"]["msg_timestamp"])})
#  newcoldata.update({"node_id_str": str(mitigation["collector"]["data"]["node_id_str"])})
#  newcoldata.update({"subscription_id_str": str(mitigation["collector"]["data"]["subscription_id_str"])})

  mitigation["collector"]["data"] = newcoldata

#Helper-Methods for CISCO
#---------------------------
def cisco_lastclear2epoch():
  if "encoding_path" in mitigation["collector"]["data"]:
    if "openconfig-interfaces:" in mitigation["collector"]["data"]["encoding_path"]: 
      if "interfaces" in mitigation:
        if "interface" in mitigation["interfaces"]:
          for intelem in mitigation["interfaces"]["interface"]:
            intidx = mitigation["interfaces"]["interface"].index(intelem)

            if "state" in mitigation["interfaces"]["interface"][intidx]:
              if "counters" in mitigation["interfaces"]["interface"][intidx]["state"]:
                for leave in mitigation["interfaces"]["interface"][intidx]["state"]["counters"]:
                  if (leave == "last_clear"):
                    try:
                      #"last_clear":"2019-01-08T12:53:02Z",
                      utc_dt = datetime.strptime(mitigation["interfaces"]["interface"][intidx]["state"]["counters"]["last_clear"], '%Y-%m-%dT%H:%M:%SZ')
                      timestamp = (utc_dt - datetime(1970, 1, 1)).total_seconds()
                    except ValueError:
                      mitigation["interfaces"]["interface"][intidx]["state"]["counters"][leave] = 0
                    else:
                      mitigation["interfaces"]["interface"][intidx]["state"]["counters"]["last_clear"] = int(timestamp)

            elif "subinterfaces" in mitigation["interfaces"]["interface"][intidx]:
              if "subinterface" in mitigation["interfaces"]["interface"][intidx]["subinterfaces"]:
                for subintelem in mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"]:
                  subintidx = mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"].index(subintelem)
                  if "state" in mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"][subintidx]:
                    if "counters" in mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"][subintidx]["state"]:
                      for leave in mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"][subintidx]["state"]["counters"]:
                        if (leave == "last_clear"):
                          try:
                            #"last_clear":"2019-01-08T12:53:02Z",
                            utc_dt = datetime.strptime(mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"][subintidx]["state"]["counters"]["last_clear"], '%Y-%m-%dT%H:%M:%SZ')
                            timestamp = (utc_dt - datetime(1970, 1, 1)).total_seconds()
                          except ValueError:
                            mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"][subintidx]["state"]["counters"]["last_clear"] = 0
                          else:
                            mitigation["interfaces"]["interface"][intidx]["subinterfaces"]["subinterface"][subintidx]["state"]["counters"]["last_clear"] = int(timestamp)
      


#Helper-Methods for HUAWEI
#---------------------------
def sens2enco():
  if "collector" in mitigation:
    if "data" in mitigation["collector"]:
      if "sensor_path" in mitigation["collector"]["data"]:
        mitigation["collector"]["data"].update({"encoding_path": mitigation["collector"]["data"]["sensor_path"]})
        del mitigation["collector"]["data"]["sensor_path"]

def insert_rec_interfaces():
  if "encoding_path" in mitigation["collector"]["data"]:
    if "openconfig-interfaces:" in mitigation["collector"]["data"]["encoding_path"]: 
      if "interface" in mitigation:
        if type(mitigation["interface"]) == list:
          mitigation.update({"interfaces": {"interface": mitigation["interface"]}})
          del mitigation["interface"]

def remove_hua_state():
  if "encoding_path" in mitigation["collector"]["data"]:
    if "openconfig-interfaces:" in mitigation["collector"]["data"]["encoding_path"]: 
      if "interfaces" in mitigation:
        if "interface" in mitigation["interfaces"]:
          interface_list = list(mitigation["interfaces"]["interface"])
          for elem in interface_list:
            idx = interface_list.index(elem)
            if "state" in interface_list[idx]:
              if "counters" in interface_list[idx]["state"]:
                mitigation["interfaces"]["interface"][idx]["state"] = {"counters": mitigation["interfaces"]["interface"][idx]["state"]["counters"]}

def int_state_counters_2_int():
  if "encoding_path" in mitigation["collector"]["data"]:
    if "openconfig-interfaces:" in mitigation["collector"]["data"]["encoding_path"]: 
      if "interfaces" in mitigation:
        if "interface" in mitigation["interfaces"]:
          mitigation["interfaces"]["interface"]
          for elem in mitigation["interfaces"]["interface"]:
            idx = mitigation["interfaces"]["interface"].index(elem)
            if "state" in mitigation["interfaces"]["interface"][idx]:
              if "counters" in mitigation["interfaces"]["interface"][idx]["state"]:
                for leave in mitigation["interfaces"]["interface"][idx]["state"]["counters"]:
                  if not (leave == "last_clear"):
                    try:
                      int(mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave])
                    except ValueError:
                      mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave] = -1
                    else:
                      mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave] = int(mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave])
                  else:
                    try:
                      int(mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave])
                    except ValueError:
                      mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave] = 0
                    else:
                      mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave] = int(mitigation["interfaces"]["interface"][idx]["state"]["counters"][leave])

def correct_huaw_enc_path():
  if "encoding_path" in mitigation["collector"]["data"]:
    if "openconfig-interfaces:" in mitigation["collector"]["data"]["encoding_path"]:
      mitigation["collector"]["data"]["encoding_path"] = "openconfig-interfaces:interfaces"



if __name__ == '__mod_all_json_data__':
  mod_all_json_data
