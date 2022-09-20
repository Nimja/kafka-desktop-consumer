import os
import subprocess

FILE_NAME_SPLIT = ' - '


class Reader:
    def __init__(self, cache_path: str):
        self.cache_path = cache_path

    def _sort_file_list(self, original: list) -> list:
        offsets = list(original)
        # Highest first.
        offsets.sort(key=self._get_int_from_file_name, reverse=True)
        return offsets

    def get_topic_path(self, topic_name):
        return os.path.join(self.cache_path, topic_name)

    def get_topic_offsets_from_cache(self, topic_name):
        topic_path = self.get_topic_path(topic_name)
        if not os.path.exists(topic_path):
            return []

        f = []
        for (dirpath, dirnames, filenames) in os.walk(topic_path):
            f.extend(filenames)
            break

        return self._sort_file_list(f)

    def get_topic_offsets_for_search(self, topic_name, search):
        topic_path = self.get_topic_path(topic_name)

        command = f"cd {topic_path} && grep -e \"{search}\" * -l"

        try:
            output = subprocess.check_output(command, shell=True)
            if output:
                output_list = output.decode('ascii').split("\n")
                self._update_list(self._sort_file_list(output_list))
                return
        except subprocess.CalledProcessError as e:
            pass

    def get_latest_topic_offset(self, topic_name):
        offsets = self.get_topic_offsets_from_cache(topic_name)
        return self._get_int_from_file_name(offsets[0]) if offsets else 0

    def _get_int_from_file_name(self, file_name):
        parts = file_name.split(FILE_NAME_SPLIT)
        if len(parts) == 2:
            return int(parts[0])
        else:
            return 0

    def get_output(self, topic_name: str, offset: str):
        """
        Get output for a single offset.
        """
        topic_path = self.get_topic_path(topic_name)

        file_name = os.path.join(topic_path, str(offset))

        result = "not found: " + file_name
        if os.path.exists(file_name) and os.path.isfile(file_name):
            with open(file_name, 'r') as f:
                result = f.read()

        return result

    def get_output_for_search(self, topic_name: str, search: str):
        """
        Search can be given as grep compatible regex.
        """
        topic_path = self.get_topic_path(topic_name)

        command = f"grep -e \"{search}\" -r {topic_path} -l"

        try:
            output = subprocess.check_output(command, shell=True)
            if output:
                output_list = output.decode('ascii').replace(topic_path + "/", '').strip().split("\n")
                return self._sort_file_list(output_list)
        except subprocess.CalledProcessError as e:
            pass

        return []
