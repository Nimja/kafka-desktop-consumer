# Kafka Client
A small desktop client to read embedded or repository AVRO serialized messages from Kafka.

It auto-decodes AVRO embedded, repository or plain JSON.

## Getting Started

Copy config.sample.ini to config.ini and configure as desired.

You can create multiple config files for different environnments!

## Requirements
OpenSSL and python >3.6 need to be installed. If you already have them, you can skip this.

* Install homebrew: https://brew.sh/
* Install openssl: `brew install openssl`
* Install python: `brew install python`
* Install python-tk: `brew install python-tk`

## Configure
* Create config: `cp config.sample.ini config.ini`
* Edit config file!

# Running

`./run.sh`

For a custom config file run with:
`./run.sh -c config_test`

It will auto-append `.ini` if that file exists.

This will:
* Create a virtual env and install pip requirements using poetry.
  * If the venv folder already exists, this step is skipped.
* Run `./main.py` to start the application.

After the venv has been created, running `./main.py` directly will also work.
