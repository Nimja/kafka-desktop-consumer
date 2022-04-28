#!./venv/bin/python3
"""
The window frame with the kafka consumer elements.
"""

import wx
import configparser
import kafka
import json
import os
from lib.wxautocompletectrl import AutocompleteTextCtrl, list_completer, EVT_VALUE_CHANGED

if not os.path.isfile('config.ini'):
    print("ERROR: Remember to copy config.sample.ini to your own config.ini file!")
    import sys

    sys.exit(1)

config = configparser.ConfigParser()
config.read('config.ini')
defaults = config['defaults']
kafka_settings = config['kafka']
avro_settings = config['avro']

class ClientFrame(wx.Frame):
    """
    A Frame that says Hello World
    """

    messages = []
    def __init__(self, *args, **kw):
        # ensure the parent's __init__ is called
        super(ClientFrame, self).__init__(*args, **kw)

        sizer_top = wx.BoxSizer(wx.HORIZONTAL)

        self.consumer = kafka.consumer.KafkaConsumer(
            kafka_settings=kafka_settings,
            avro_settings=avro_settings,
            limit=defaults['max.messages']
        )

        # Add topic field, an auto-complete field.
        self.topic = AutocompleteTextCtrl(
            self,
            completer=list_completer(self.consumer.get_topic_list()),
            style=wx.TE_PROCESS_ENTER
        )
        self.topic.SetValue(defaults.get('topic'))
        self.Bind(wx.EVT_TEXT_ENTER, self.refresh_topic, self.topic)
        self.topic.onchange = self._update_messages

        sizer_top.Add(self.topic, 1, wx.EXPAND)

        # Add offset field.
        self.offset = wx.TextCtrl(self, style=wx.TE_PROCESS_ENTER, size=(50, -1))
        self.offset.SetValue(defaults.get('offset', '0'))
        self.Bind(wx.EVT_TEXT_ENTER, self.refresh_topic, self.offset)

        sizer_top.Add(self.offset, 0, wx.EXPAND)

        # Add update button.
        self.update_button = wx.Button(self, -1, "Update", size=(100, -1))
        self.Bind(wx.EVT_BUTTON, self.refresh_topic, self.update_button)
        sizer_top.Add(self.update_button, 0, wx.EXPAND)

        self.export_button = wx.Button(self, -1, "Export ALL", size=(100, -1))
        self.Bind(wx.EVT_BUTTON, self.export_data, self.export_button)
        sizer_top.Add(self.export_button, 0, wx.EXPAND)

        sizer_bottom = wx.BoxSizer(wx.HORIZONTAL)

        # Message list.
        self.list = wx.ListBox(self, size=(200, -1), style=wx.LB_SINGLE)
        self.Bind(wx.EVT_LISTBOX, self.show_item, self.list)
        sizer_bottom.Add(self.list, 0, wx.EXPAND)

        # Json output.
        self.output = wx.TextCtrl(self, size=(500, 450), style=wx.TE_MULTILINE | wx.TE_READONLY)
        sizer_bottom.Add(self.output, 1, wx.EXPAND)
        self.sizer_bottom = sizer_bottom

        # Basic sizer.
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.sizer.Add(sizer_top, 0, wx.EXPAND)
        self.sizer.Add(sizer_bottom, 1, wx.EXPAND)
        self.SetSizer(self.sizer)

        self.SetAutoLayout(True)
        self.sizer.Fit(self)

        # and a status bar
        self.CreateStatusBar()
        self.SetStatusText("... loading ...")

        self.Show()

    def update_topic(self, event):
        self.show_error("Updated topic.")

    def refresh_topic(self, event):
        self._update_messages()

    def show_item(self, event):
        """
        Show single item in main (big) pane.
        :param event:
        :return:
        """
        index = self.list.GetSelection()
        if index is None or not self.messages:
            self.output.SetValue('')
        else:
            real_index = len(self.messages) - 1 - index
            item = self.messages[real_index]
            self.output.SetValue(self._to_json_string(item['value']))

    def show_error(self, string):
        """
        Show error with popup.
        :param string:
        :return:
        """
        wx.MessageBox(string, style=wx.ICON_ERROR | wx.CENTRE)

    def export_data(self, event):
        """ Export all data. """

        with wx.FileDialog(self, "Save file", defaultFile="%s.json" % self.topic.GetValue(),
                           style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT) as fileDialog:

            if fileDialog.ShowModal() == wx.ID_CANCEL:
                return  # Don't continue, user cancelled.

            # Save to file.
            pathname = fileDialog.GetPath()
            try:
                with open(pathname, 'w') as file:
                    file.write(self._get_export_data())
            except IOError:
                self.show_error("Unable to save in: %s" % pathname)

    def _get_export_data(self):
        """
        Get export data of all current messages in json string.
        :return:
        """
        json_dict = {}
        for message in self.messages:
            json_dict["%s - %s" % (message['offset'], message['key'])] = message['value']

        return self._to_json_string(json_dict)

    def _to_json_string(self, data):
        return json.dumps(data, sort_keys=False, indent=4)

    def _update_messages(self):
        topic_name = self.topic.GetValue()
        offset = int(self.offset.GetValue())
        self.offset.SetValue(str(offset))
        if not topic_name:
            self.SetStatusText("No topic given...")
            return

        self.SetStatusText("Loading messages from Kafka...")
        self.sizer.Hide(self.sizer_bottom)
        # Update the window.
        app.Yield()
        try:
            self.messages = self.consumer.consume([topic_name], offset=offset)
            choices = []
            for message in self.messages:
                choices.insert(0, "%s - %s" % (message['offset'], message['key']))

            self.list.Set(choices)
            if choices:
                self.list.SetSelection(0)
            self.show_item(None)
            self.sizer.Show(self.sizer_bottom)
            self.SetStatusText("Topic: %s - Count: %s" % (self.topic.GetValue(), len(self.messages)))
        except Exception as e:
            self.SetStatusText("ERROR!")
            self.show_error(str(e))


app = wx.App()
frm = ClientFrame(None, title='Kafka Consumer', size=(500, 500))
app.MainLoop()
