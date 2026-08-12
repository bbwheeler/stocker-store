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

## Task Instructions

### Beginning Tasks
Before you begin, follow these steps:
1. If there are no uncommitted changes, switch to the main branch.
2. If there are uncommitted changes, commit them on a branch and then push to remote origin (git.wheeli.ca).
3. Do a git fetch so that you have all of the latest changes. Then if you are on a branch, merge main into your branch.
4. If on main, create a branch appropriate for the changes that you will make.

### Finishing Tasks
Once you complete a task, follow these steps:
1. Commit the code into a branch using git
2. Push the code to remote origin (git.wheeli.ca)
3. Open a merge request for the changes you just pushed
4. Add me (username: brian) as a reviewer on the merge request

## Credentials
Your credentials for the Forgejo instance git.wheeli.ca can be found in the parent directory (../credentials.md)