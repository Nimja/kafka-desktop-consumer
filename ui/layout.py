from tkinter import Scrollbar
import PySimpleGUI as sg
from . import main

# Setup window layout.
layout = [
    [
        sg.Text("Kafka Topic"),
        sg.Button("Switch", key=main.BUTTON_CHANGE_TOPIC),
        sg.Text(size=(25, 1), key=main.OUTPUT_TOPIC, expand_x=True),
        sg.Input(size=(10, 1), key=main.INPUT_OFFSET, enable_events=True),
        sg.Button("Load", key=main.BUTTON_LOAD),
    ],
    [
        sg.Column(
            [
                [sg.Text("Filter")],
                [sg.In(size=(25, 1), key=main.INPUT_SEARCH, enable_events=True, expand_x=True)],
                [sg.Text("Messages")],
                [
                    sg.Listbox(
                        values=[],
                        enable_events=True,
                        key=main.OUTPUT_LIST,
                        select_mode=sg.LISTBOX_SELECT_MODE_SINGLE,
                        expand_x=True,
                        expand_y=True
                    ),
                ],
            ],
            pad=(5, 5),
            expand_x=True,
            expand_y=True
        ),
        sg.Column(
            [
                [sg.Text("Contents")],
                [
                    sg.Multiline(
                        key=main.OUTPUT_CONTENT,
                        disabled=True,
                        expand_x=True,
                        expand_y=True
                    )
                ]
            ],
            key=main.OUTPUT_COLUMN,
            expand_x=True,
            expand_y=True
        )
    ],
    [
        sg.Text("Status: ", key=main.STATUS_TEXT),
    ]
]
