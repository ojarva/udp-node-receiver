import datetime
import json
import Queue
import re
import redis
import socket
import threading
import time

UDP_IP = "0.0.0.0"
UDP_PORT = 8225


class StatsReceiver(object):
    TEMPERATURE_RE = re.compile(r"^.*-t\d$")
    POWER_READING_RE = re.compile(r"^E\d+")
    PIR_RE = re.compile("^.*pir$")
    SWITCH_RE = re.compile("^.*-sw$");
    PM_RE = re.compile("^.*pm\d+$")

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET,  # Internet
                                  socket.SOCK_DGRAM)  # UDP
        self.sock.bind((UDP_IP, UDP_PORT))
        self.last_values = {}
        self.send_queue = Queue.Queue()
        self.timer = None
        self.redis = redis.StrictRedis()
        self.running = False
        self.node_value_sets = {}

    def send_timer(self):
        last_sent_at = time.time()
        while True:
            if not self.running:
                return
            time.sleep(3)
            items = []
            while True:
                try:
                    items.append(self.send_queue.get_nowait())
                except Queue.Empty:
                    break
            if len(items) > 0:
                print "Sending", items
                self.redis.publish("influx-update-pubsub", json.dumps(items))

    def process(self, node_name, key, value):
                item_type = "generic"
                fields = {}
                if self.TEMPERATURE_RE.match(key):
                    value = float(value) / 100
                    item_type = "temperature"
                    if value < -70 or value == 127:
                        print "Invalid value for temperature: %s degC, key: %s" % (value, key)
                        return
                elif self.PIR_RE.match(key):
                    value = value == "1"
                    item_type = "pir"
                elif self.POWER_READING_RE.match(key):
                    value = float(value) * 230
                    item_type = "watts"
                    if value < 0 or value > 5000:
                        print "Invalid value for power consumption: %s W, key %s" % (value, key)
                elif node_name == "tea" and key == "nfc-id":
                    item_type = "tea-reader"
                elif self.SWITCH_RE.match(key):
                    value = value == "1"
                    item_type = "switch"
                elif self.PM_RE.match(key):
                    value = int(value)
                    item_type = "dust_pm"
                elif key == "dust":
                    value = int(value)
                    item_type = "dust"

                fields[item_type] = value

                output = {
                    "time": datetime.datetime.utcnow().isoformat() + "Z",
                    "measurement": "node-" + node_name,
                    "tags": {
                        "key": key,
                    },
                    "fields": fields,
                }
                if node_name not in self.node_value_sets:
                    self.node_value_sets[node_name] = {}
                self.node_value_sets[node_name][key] = value
                self.redis.publish("watchdog-input", json.dumps({"name": "node-%s" % node_name, "values": self.node_value_sets[node_name]}))
                self.redis.publish("node-%s-pubsub" % node_name, json.dumps({"name": "node-%s" % node_name, "key": key, "value": value, "item_type": item_type}))
                if item_type == "pir" or item_type == "switch":
                    if value:
                        self.redis.publish("lightcontrol-triggers-pubsub", json.dumps({"key": node_name, "trigger": item_type}))
                    if key not in self.last_values:
                        self.last_values[key] = {"value": None, "seen": datetime.datetime.now()}
                    if self.last_values[key]["value"] == value and datetime.datetime.now() - self.last_values[key]["seen"] < datetime.timedelta(seconds=120):
                        return
                    self.last_values[key]["value"] = value
                    self.last_values[key]["seen"] = datetime.datetime.now()
                elif item_type == "tea-reader":
                    self.redis.publish("tea-reader-pubsub", json.dumps({"id": value}))

                self.send_queue.put(output)

    def run(self):
        try:
            self.running = True
            self.timer = threading.Thread(target=self.send_timer)
            self.timer.start()
            while True:
                data, addr = self.sock.recvfrom(1024)  # buffer size is 1024 bytes
                splitted_data = data.split(":")
                if len(splitted_data) != 2:
                    print "Malformed data from {sender}: {data}".format(sender=addr, data=data)
                    continue
                key = splitted_data[0]
                splitted_key = key.split(".")
                node_name = splitted_key[0]
                if len(splitted_key) < 2:
                    print "Malformed key from {sender}: {data}".format(sender=addr, data=data)
                    continue
                value = splitted_data[1].strip()
                if "|" in value:
                    values = value.split("|")
                    for value in values:
                        item_key, item_value = value.split("=")
                        self.process(node_name, item_key, item_value)
                else:
                    key = ".".join(splitted_key[1:])
                    self.process(node_name, key, value)
        except KeyboardInterrupt as err:
            self.running = False
            raise err

    def close(self):
        if self.sock:
            self.sock.close()


def main():
    stats_receiver = StatsReceiver()
    stats_receiver.run()


if __name__ == '__main__':
    main()
