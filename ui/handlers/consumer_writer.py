import json
import os
from kafka.consumer import KafkaConsumer
from datetime import datetime, timezone
from .reader import Reader
from ..dicthelper import get_dict_by_path

FILE_NAME_SPLIT = ' - '

class ConsumerWriter:
    def __init__(self, parent_ui, consumer: KafkaConsumer, reader: Reader, convert_unix_ts_path):
        self.parent_ui = parent_ui
        self.consumer = consumer
        self.reader = reader
        self.convert_unix_ts_path = convert_unix_ts_path

    def load_topic(self, topic_name, offset_start, search_key, limit=None):
        # Update limit if appropriate.
        if limit and isinstance(limit, int) and limit > 0:
            self.consumer.limit = limit

        self._update_status('Loading...')
        self.has_results = False

        topic_path = self.reader.get_topic_path(topic_name)

        path_exists = os.path.exists(topic_path)

        is_searching = search_key
        latest_offset = offset_start if offset_start > 0 else 0

        if not path_exists:
            os.makedirs(topic_path)

        self._update_status("Loading keys...")
        total_key_list = self._load_keys_from_file(topic_path)
        counters = {
            "count": 0,
            "found": 0,
        }
        search_offsets, latest_offset = self._search_total_key_list(
            topic_path,
            total_key_list,
            search_key,
            latest_offset,
            counters
        )

        # Go over generator.
        # Search existing offsets.
        if search_offsets:
            self._update_status("Loading cached key messages...")
            for search_offset in search_offsets:
                for offset, key, timestamp, item in self.consumer.consume(topic_name, search_offset, search_key, 1):
                    self._handle_single_message(
                        topic_path,
                        counters,
                        total_key_list,
                        is_searching,
                        offset, key,
                        timestamp,
                        item,
                        direct_search=True
                    )
        # Loading normally.
        self._update_status("Loading...")
        for offset, key, timestamp, item in self.consumer.consume(topic_name, latest_offset, search_key):
            self._handle_single_message(
                topic_path,
                counters,
                total_key_list,
                is_searching,
                offset, key,
                timestamp,
                item
            )

        self._write_keys_to_file(topic_path, total_key_list)

    def _search_total_key_list(self, topic_path, total_key_list, search_key, latest_offset, counters):
        search_offsets = []
        if total_key_list and search_key:
            self._update_status("Searching known keys...")
            for search_offset, data in total_key_list.items():
                search_offset = int(search_offset)
                if search_key in str(data[0]):
                    if self._cached_message_exists(topic_path, search_offset, data[0]):
                        counters["found"] += 1
                    else:
                        search_offsets.append((search_offset, data[0]))
                latest_offset = max(latest_offset, search_offset)
        return search_offsets, latest_offset

    def _handle_single_message(
        self,
        topic_path,
        counters,
        total_key_list,
        is_searching,
        offset, key,
        timestamp,
        item,
        direct_search=False
    ):
        ts = self._get_string_from_timestamp(timestamp[1])
        if item:
            total_key_list[offset] = (key, ts)
            self._write_item_to_file(topic_path, item)
            counters["found"] += 1
        else:
            total_key_list[offset] = (key, ts)
        counters["count"] += 1
        if counters["count"] % 10 == 0 or direct_search:
            count = counters["count"]
            found = counters["found"]
            status_message = f"Loading... {count:,} - Offset: {offset:,} - Time: {ts}"
            if is_searching:
                status_message += f" - Found: {found:,}"
            self._update_status(status_message)

    def _update_status(self, status_message):
        self.parent_ui.update_status(status_message)

    def _load_keys_from_file(self, topic_path):
        file_name = topic_path + ".json"
        if not os.path.exists(file_name) or not os.path.isfile(file_name):
            return {}

        # Write json file away.
        with open(file_name, 'r') as f:
            return json.loads(f.read())

    def _write_keys_to_file(self, topic_path, total_key_list):
        file_name = topic_path + ".json"

        # Write json file away.
        with open(file_name, 'w') as f:
            f.write(json.dumps(total_key_list, indent=4))

    def _cached_message_exists(self, topic_path, offset, key):
        file_name = self._create_message_file_name(topic_path, offset, key)
        return os.path.exists(file_name) and os.path.isfile(file_name)

    def _create_message_file_name(self, topic_path, offset, key):
        base_name = f"{offset}{FILE_NAME_SPLIT}{key}.json"
        return os.path.join(topic_path, base_name)

    def _write_item_to_file(self, topic_path, item):
        file_name = self._create_message_file_name(topic_path, item['offset'], item['key'])

        # Write json file away.
        with open(file_name, 'w') as f:
            self._translate_unix_timestamp(item)
            f.write(json.dumps(item, indent=4))

    def _translate_unix_timestamp(self, item):
        if not self.convert_unix_ts_path:
            return

        handle, last = get_dict_by_path(item, self.convert_unix_ts_path, {})
        # Check final value.
        if last not in handle:
            return

        handle[last + "_converted"] = self._get_string_from_timestamp(
            int(handle[last])
        )

    @staticmethod
    def _get_string_from_timestamp(ts):
        if ts > 9999999999:  # If timestamp is in milliseconds.
            ts = round(ts / 1000)
        # Get datetime as UTC
        dt = datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
        # Convert to string and return
        return dt.astimezone().strftime('%Y-%m-%d %H:%M:%S')
