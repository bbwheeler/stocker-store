# Stocker Store

## Description

**Stocker Store** is a gRPC-backed service for managing stock data and scores. It supports thousands of stocks and flexible retrieval patterns.

## Requirements

* Retrieve stocks by: symbol, exchange, score ranges
* Submit stocks.
* Remove stocks from an exchange.
* Submit scores (dynamic categories, normalized -1.0 to 1.0).
* Thousands of stocks.
* If request limits are less than the total, a random set will be provided
* Self-hostable using Podman Quadlets
* simple, concise

## Tech

* gRPC
* PostgreSQL
* Go

## Version Control

## Submitting Changes

All changes, edits, documents, and artifacts must be pushed to the repository when complete. To do so, these steps must be followed:
1. Commit the code into a branch using git
2. Push the code to remote origin (git.wheeli.ca)
3. Open a pull request for the changes you just pushed
4. Add me (username: brian) as a reviewer on the merge request

## Tools

The Forgeji MCP (command: forgejo_mcp) can be used to execute tasks on git.wheeli.ca such as putting up a PR or MR.

Credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)