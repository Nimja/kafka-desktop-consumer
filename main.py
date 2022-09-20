#!./venv/bin/python3
import PySimpleGUI as sg
from ui.layout import layout
from ui.main import Main
from ui.config import get_config
from kafka.consumer import KafkaConsumer

# Get config and cache path.
config, cache_path = get_config()

# Get kafka consumer, which is passed to window.
consumer = KafkaConsumer(config['kafka'], config['avro'], limit=1000000)

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
main = Main(config, window, consumer, cache_path)

# Main event loop.
while True:
    event, values = window.read()
    if event == sg.WINDOW_CLOSED:
        break
    main.handle(event, values)

window.close()