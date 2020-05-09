# pykafkaadmintools
Python Flask based Kafka Cluster Administration Tool

## Introduction

Currently we do not have a simple Kafka Cluster administration tool available and ditributed with Apache Kafka. This is a simple Python Flask based Kafka Administration tool which publish Rest interface to perform different Kafka cluster administration tasks.


<ins>***Features***</ins>

- `Rest endpoints` for all major Kafka administration operations. Enables complete remote administration.
- `Swagger UI` for human interaction to the tool.
- Support for `Topic` creation, deletion, update and configuration.
- Support Zookeeper (Native) based Topic Authorisation (`ACL`) policy creation, deletion, updation and listing.
- Supports Kafka Cluster `services` (Kafka, Zookeeper, etc) management like start, stop, status check.
- Supports creation, deletion, update `Kafka Broker configuration`.
- Suports Transport layer security and Authentication (file based out of the box) for Rest endpoints.

## Installation

Below is the requirement of the host running the tool.

- Connectivity to the Kafka Cluster (note you need connectivity for Kafka and SSH service).
- Python 3 installed. 


<ins>***Installation Steps***</ins>

- Download or Clone this repository.
```
git clone https://github.com/sankamuk/pykafkaadmintools.git
```

- Install required library listed in requirements file using PIP.
```
pip install -r requirements.txt 
```

- Configure tool by updating `tools.config`. Detail in [Wiki page](https://github.com/sankamuk/pykafkaadmintools/wiki/Configure-Tool).


- Start the tool.
```
nohup python app.py & 
```

## Usage

Once started you should have the swagger available (with documentation of the individual REST endpoints) in below url.


`http[s]://[app.hostname]:[app.port]`


## Troubleshoot

While running you can always run into issue and the best way to resolve issue is by looking into the log. The tool should log all its action to a log file name `pykafkaadmintools.log` in the same directory.


## Usage Demo

I will try to provide some demonstration of usage in the [blog](https://mukherjeesankar.wordpress.com/2020/05/09/setup-kafka-administer-remotely-with-kafkaadmintools/).

