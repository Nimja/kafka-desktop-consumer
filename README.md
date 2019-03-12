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

## Running

```bash
python3 main.py
```