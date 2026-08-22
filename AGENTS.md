# Stocker Store

## Description

**Stocker Store** is a service for managing stock data and scores. It supports thousands of stocks and flexible retrieval patterns.

## Requirements

* Retrieve stocks by: symbol, exchange, score ranges
* Submit stocks.
* Remove stocks from an exchange.
* Submit scores (dynamic categories, normalized -1.0 to 1.0).
* Thousands of stocks.
* If request limits are less than the total, a random set will be provided
* Self-hostable using Podman Quadlets
* Remove stale stocks after a configurable duration
* Receive stocks through gRPC or Kafka
* simple, concise

## Tech

* gRPC
* PostgreSQL
* Go
* Kafka

## Tools

Credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)