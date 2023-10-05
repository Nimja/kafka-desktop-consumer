import re
from .handlers.consumer_writer import ConsumerWriter
from .handlers.reader import Reader

BUTTON_CHANGE_TOPIC = 'button_change_topic'

OUTPUT_TOPIC = 'output_topic'
BUTTON_LOAD = 'button_load'

INPUT_SEARCH = 'input_search'
INPUT_SCAN = 'input_scan'
BUTTON_SCAN = 'button_scan'
INPUT_OFFSET = 'input_offset'
BUTTON_SEARCH = 'button_search'

OUTPUT_LIST = 'output_list'
OUTPUT_CONTENT = 'output_content'
OUTPUT_COLUMN = 'output_column'

STATUS_TEXT = 'status_text'

TAB_FILTER = 'tab_filter'
TAB_SCAN = 'tab_scan'

FILE_NAME_SPLIT = ' - '


class Main:
    def __init__(self, config, window, consumer, cache_path) -> None:
        self.window = window
        self.consumer = consumer
        self.reader = Reader(cache_path)
        self.consumer_writer = ConsumerWriter(
            parent_ui=self,
            consumer=consumer,
            reader=self.reader,
            convert_unix_ts_path=config.get('convert_unix_ts_path', '')
        )
        self.topic_list = consumer.get_topic_list()
        self.has_results = False
        self.is_selecting_topic = False
        self.full_offsets = []
        self.current_offset = 0
        self.init(config.get('topic', ''))

    def handle(self, event, values):
        # Get the focus element keyname.
        focus_element_obj = self.window.find_element_with_focus()
        focus_element = focus_element_obj.Key if focus_element_obj else None

        # Handle load button.
        if event == BUTTON_LOAD:
            self._load_topic()

        elif event == BUTTON_CHANGE_TOPIC:
            self._toggle_topic_selection()
            self._update_list()

        # Changing offset.
        elif event == INPUT_OFFSET:
            self._update_offset(values[INPUT_OFFSET])

        # Searching in cached output.
        elif event == BUTTON_SEARCH or focus_element == INPUT_SEARCH:
            search_text = values[INPUT_SEARCH]
            self._get_output_for_search(search_text)

        # Searching in cached output.
        elif event == BUTTON_SCAN:
            scan_text = values[INPUT_SCAN]
            self._update_list_for_scan(scan_text)

        # Handle click in list.
        elif event == OUTPUT_LIST:
            if self.has_results:
                self._update_output_content(values[OUTPUT_LIST][0])

    def init(self, topic_name):
        if self._attempt_topic(topic_name):
            self.full_offsets = self.reader.get_topic_offsets_from_cache(self.topic_name)
            self._auto_update_offset()
            self._update_list()

    def _attempt_topic(self, topic_name):
        self.has_results = False
        if topic_name not in self.topic_list:
            self.topic_name = False
            self.update_status("Topic not found...?")
            return False
        else:
            self._toggle_topic_selection(False)
            self.window[OUTPUT_TOPIC].update(topic_name)
            self.topic_name = topic_name
            return True

    def _toggle_topic_selection(self, is_selecting=None):
        if is_selecting == None:
            is_selecting = not self.is_selecting_topic

        self.window[OUTPUT_COLUMN].update(visible=not is_selecting)
        self.window[TAB_SCAN].update(visible=not is_selecting)
        self.window[BUTTON_SCAN].update(disabled=is_selecting)
        self.window[INPUT_SCAN].update(disabled=is_selecting)
        self.is_selecting_topic = is_selecting
        self.window[INPUT_SEARCH].update('')

    def _load_topic(self, search_key=None):
        if not self.topic_name:
            self.update_status("Topic not found...?")
            return
        # Clear output windows.
        self.window[INPUT_SEARCH].update('')
        self.window[OUTPUT_CONTENT].update('')
        self.window[OUTPUT_LIST].update([''])
        # Load.
        self.consumer_writer.load_topic(self.topic_name, self.current_offset, search_key)
        self.full_offsets = self.reader.get_topic_offsets_from_cache(self.topic_name)
        self._update_list()
        self._auto_update_offset()

    def _update_list(self, offsets=None):
        # By default, load the cached offsets.
        if offsets == None:
            offsets = self._get_current_full_list()
        result_count = len(offsets)
        # Set result flag.
        self.has_results = result_count > 0

        if self.is_selecting_topic:  # Topic switching mode just lists all topics.
            self.update_status(f"Pick a topic...")
            # Only show the first (latest) 1000 items to avoid window lagging.
            self.window[OUTPUT_LIST].update(offsets)
        else:
            self.update_status(f"Results: {result_count:,} in topic: {self.topic_name}")
            # Only show the first (latest) 1000 items to avoid window lagging.
            self.window[OUTPUT_LIST].update(offsets[:1000])

            # Show top one in output.
            if offsets:
                self._update_output_content(offsets[0])
            else:
                self.window[OUTPUT_CONTENT].update('')

    def _update_list_for_scan(self, scan_text):
        # By default, load the cached offsets.
        if scan_text == None:
            return; # Do nothing.
        self._load_topic(scan_text)

    def _get_current_full_list(self):
        if self.is_selecting_topic:
            return self.topic_list
        else:
            return self.full_offsets

    def _update_output_content(self, selection: str):
        # If we're in topic switching mode, try to load it.
        if self.is_selecting_topic:
            if self._attempt_topic(selection):
                self._load_topic()
            return

        # Else, attempt to show the output.
        if (self.has_results):
            result = self.reader.get_output(self.topic_name, selection)
        else:
            result = 'Not loaded...'
        self.window[OUTPUT_CONTENT].update(result)

    def _get_output_for_search(self, search: str):
        """
        Simple in-string searching.
        """
        full_list = self._get_current_full_list()
        if not full_list:
            return  # Nothing to seach.

        if search:  # Search with in string.
            case_search = search.casefold()
            offsets = list(filter(lambda v: case_search in v.casefold(), full_list))
            self._update_list(offsets)
        else:  # Return to full set.
            self._update_list()

    def update_status(self, text):
        self.window[STATUS_TEXT].update(text)
        self.window.refresh()

    def _auto_update_offset(self):
        offset = self.reader.get_latest_topic_offset(self.topic_name)
        self.window[INPUT_OFFSET].update(offset)
        self.current_offset = offset
        self.window.refresh()

    def _update_offset(self, new_offset):
        new_offset = re.sub('\D', '', new_offset)
        if new_offset:
            new_offset = int(new_offset)
        else:
            new_offset = 0
        self.window[INPUT_OFFSET].update(str(new_offset))
        self.current_offset = new_offset
