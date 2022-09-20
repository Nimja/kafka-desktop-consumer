import json
import os
from datetime import datetime, timezone
from .reader import Reader

FILE_NAME_SPLIT = ' - '


class ConsumerWriter:
    def __init__(self, parent_ui, consumer, reader: Reader, convert_unix_ts_path):
        self.parent_ui = parent_ui
        self.consumer = consumer
        self.reader = reader
        self.convert_unix_ts_path = convert_unix_ts_path

    def load_topic(self, topic_name):
        self.parent_ui.update_status('Loading...')
        self.has_results = False
        topic_path = self.reader.get_topic_path(topic_name)

        path_exists = os.path.exists(topic_path)

        latest_offset = 0
        if path_exists:  # We have previous messages.
            latest_offset = self.reader.get_latest_topic_offset(topic_name)

        if not path_exists:
            os.makedirs(topic_path)

        # Go over generator.
        count = 0
        for item in self.consumer.consume(topic_name, latest_offset):
            self._write_item_to_file(topic_path, item)
            count += 1
            if count % 10 == 0:
                self.parent_ui.update_status(f"Loading... {count:,}")

    def _write_item_to_file(self, topic_path, item):
        base_name = f"{item['offset']}{FILE_NAME_SPLIT}{item['key']}.json"
        file_name = os.path.join(topic_path, base_name)

        # Write json file away.
        with open(file_name, 'w') as f:
            self._translate_unix_timestamp(item)
            f.write(json.dumps(item, indent=4))

    def _translate_unix_timestamp(self, item):
        if not self.convert_unix_ts_path:
            return

        parts = self.convert_unix_ts_path.split("/")
        last = parts.pop()
        handle = item
        # Traverse path.
        for part in parts:
            if part in handle:
                handle = handle[part]
            else:
                return

        # Check final value.
        if last not in handle:
            return

        ts = int(handle[last])
        if ts > 9999999999:  # If timestamp is in milliseconds.
            ts = round(ts / 1000)
        # Get datetime as UTC
        dt = datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
        # Convert to string and store in object.
        handle[last + "_converted"] = dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')
