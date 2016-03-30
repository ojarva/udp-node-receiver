import datetime
import json
import Queue
import re
import redis
import socket
import threading
import time
import logging

UDP_IP = "0.0.0.0"
UDP_PORT = 8225


class StatsReceiver(object):
    TEMPERATURE_RE = re.compile(r"^.*-t\d$")
    POWER_READING_RE = re.compile(r"^E\d+")
    PIR_RE = re.compile("^.*pir$")

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
        self.logger = logging.getLogger("udp-node-receiver")
        self.logger.setLevel(logging.INFO)
        format_string = "%(asctime)s - {ip}:{port} - %(levelname)s - %(message)s".format(ip=UDP_IP, port=UDP_PORT)
        formatter = logging.Formatter(format_string)
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

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
                self.logger.debug("Sending", items)
                self.redis.publish("influx-update-pubsub", json.dumps(items))

    def process(self, node_name, key, value):
        item_type = "generic"
        fields = {}
        if self.TEMPERATURE_RE.match(key):
            value = float(value) / 100
            item_type = "temperature"
            self.logger.debug("Found temperature %s, /100", value)
        elif self.PIR_RE.match(key):
            value = value == "1"
            item_type = "pir"
            self.logger.debug("Found PIR: %s", value)
        elif self.POWER_READING_RE.match(key):
            value = float(value) * 230
            item_type = "watts"
            self.logger.debug("Found amps, converted to watts: %s", value)
        elif node_name == "tea" and key == "nfc-id":
            item_type = "tea-reader"
            self.logger.debug("Found NFC tag")
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
        if item_type == "pir":
            if value:
                self.redis.publish("lightcontrol-triggers-pubsub", json.dumps({"key": node_name, "trigger": "pir"}))
            else:
                self.logger.info("Didn't trigger lightcontrol - PIR reading is False")
            if key not in self.last_values:
                self.last_values[key] = {"value": None, "seen": datetime.datetime.now() - datatime.timedelta(seconds=31)}
            time_diff = datetime.datetime.now() - self.last_values[key]["seen"]
            if self.last_values[key]["value"] == value and time_diff < datetime.timedelta(seconds=30):
                self.logger.debug("Skip sending %s - value didn't change and last seen %ss ago", output, time_diff.total_seconds())
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
                    self.logger.warning("Malformed data from %s: %s", addr, data)
                    continue
                key = splitted_data[0]
                splitted_key = key.split(".")
                node_name = splitted_key[0]
                if len(splitted_key) < 2:
                    self.logger.warning("Malformed key from %s: %s", addr, data)
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
