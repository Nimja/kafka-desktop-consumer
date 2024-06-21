#!./.venv/bin/python3
import PySimpleGUI as sg
from ui.layout import layout
from ui.main import Main
from ui.config import config
from kafka.consumer import KafkaConsumer

# Get kafka consumer, which is passed to window.
consumer = KafkaConsumer(
    config.get('kafka', {}),
    config.get('avro', {}),
    limit=int(config.get("main", {}).get('limit'))
)

# Init window with layout.
window = sg.Window(
    'Kafka Client',
    layout,
    size=(600, 600),
    return_keyboard_events=True,
    finalize=True,
    resizable=True
)

# Init main class
main = Main(config, window, consumer)

# Main event loop.
while True:
    event, values = window.read()
    if event == sg.WINDOW_CLOSED:
        break
    main.handle(event, values)

window.close()
