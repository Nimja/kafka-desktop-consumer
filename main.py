#!/bin/python
"""
Hello World, but with more meat.
"""

import wx
import configparser
import kafka
import json

config = configparser.ConfigParser()
config.read('config.ini')
defaults = config['defaults']
kafka_settings = config['kafka']


class ClientFrame(wx.Frame):
    """
    A Frame that says Hello World
    """

    messages = []

    def __init__(self, *args, **kw):
        # ensure the parent's __init__ is called
        super(ClientFrame, self).__init__(*args, **kw)

        sizer_top = wx.BoxSizer(wx.HORIZONTAL)

        # Add topic field.
        self.topic = wx.TextCtrl(self, style=wx.TE_PROCESS_ENTER)
        self.topic.SetValue(defaults.get('topic'))
        self.Bind(wx.EVT_TEXT_ENTER, self.refresh_topic, self.topic)
        sizer_top.Add(self.topic, 1, wx.EXPAND)

        # Add update button.
        self.update_button = wx.Button(self, -1, "Update", size=(100,-1))
        self.Bind(wx.EVT_BUTTON, self.refresh_topic, self.update_button)
        sizer_top.Add(self.update_button, 0, wx.EXPAND)

        sizer_bottom = wx.BoxSizer(wx.HORIZONTAL)

        # Message list.
        self.list = wx.ListBox(self, size=(200,-1), style=wx.LB_SINGLE)
        self.Bind(wx.EVT_LISTBOX, self.show_item, self.list)
        sizer_bottom.Add(self.list, 0, wx.EXPAND)

        # Json output.
        self.output = wx.TextCtrl(self, size=(500,450), style=wx.TE_MULTILINE | wx.TE_READONLY)
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
        self._update_messages()

    def update_topic(self, event):
        self.show_error("Updated topic.")

    def refresh_topic(self, event):
        self._update_messages()

    def show_item(self, event):
        index = self.list.GetSelection()
        if index is None or not self.messages:
            self.output.SetValue('')
        else:
            real_index = len(self.messages) - 1 - index
            item = self.messages[real_index]
            self.output.SetValue(json.dumps(item['value'], sort_keys=False, indent=4))

    def show_error(self, string):
        wx.MessageBox(string, style=wx.ICON_ERROR | wx.CENTRE)

    def _update_messages(self):
        topic_name = self.topic.GetValue()
        if not topic_name:
            self.SetStatusText("No topic given...")
            return

        self.SetStatusText("Loading messages from Kafka...")
        self.sizer.Hide(self.sizer_bottom)
        # Update the window.
        app.Yield()
        try:
            consumer = kafka.consumer.KafkaConsumer(kafka_settings, defaults['max.messages'])
            self.messages = consumer.consume([topic_name])
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
frm = ClientFrame(None, title='Kafka Consumer', size=(500,500))
app.MainLoop()
