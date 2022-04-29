# Kafka Client
A small desktop client to read embedded AVRO serialized messages from Kafka.

The code is quite simple and might be extended with more decoding options.

## Getting Started

Copy config.sample.ini to config.ini and configure as desired.

## Installing on OSX
* Install homebrew: https://brew.sh/
* Install openssl: `brew install openssl`
* Install python: `brew install python`
* Setup venv: `/usr/local/bin/python3 -m venv ./venv`
* Start venv: `source ./venv/bin/activate`
* Instal requirements: `pip install -r requirements.txt`

## Configure
* Create config: `cp config.sample.ini config.ini`
* Edit config file!

# Running

* Run main: `./run.sh` - This creates the virtual env (if not existing) and runs `./main.py`

After one run, you can also run `./main.py` directly.

## Listing all topics with messages
Because sometimes (especially on test environments), it's nice to know all topics with at least 1 message.

* List with messages: `./list_topics.py` - This will take a LONG time.
* List all topics: `./list_topics.py 1` - Should be very fast.
