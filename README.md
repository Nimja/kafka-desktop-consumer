# Kafka Client
A small desktop client to read embedded AVRO serialized messages from Kafka.

The code is quite simple and might be extended with more decoding options.

## Getting Started

Copy config.sample.ini to config.ini and configure as desired.

## Requirements
OpenSSL and python >3.6 need to be installed. If you already have them, you can skip this.

* Install homebrew: https://brew.sh/
* Install openssl: `brew install openssl`
* Install python: `brew install python`

## Configure
* Create config: `cp config.sample.ini config.ini`
* Edit config file!

# Running

`./run.sh`

This will:
* Create a virtual env and install pip requirements.
  * If the venv folder already exists, this step is skipped.
* Run `./main.py` to start the application.

After the venv has been created, running `./main.py` directly will also work.

## Listing all topics with messages
Because sometimes (especially on test environments), it's nice to know all topics with at least 1 message.

* List with messages: `./list_topics.py` - This will take a LONG time.
* List all topics: `./list_topics.py 1` - Should be very fast.
