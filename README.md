# How to quickly deliver data to business users

## Setup

### Pre-requisites

1. [Docker and docker compose](https://docs.docker.com/compose/install/)

Clone this repo, cd into it and start the docker containers as shown below:

```bash
git clone
cd data_engineering_systems
docker compose up -d
sleep 30 # wait 30s to give docker to start up
```

The docker compose will also start a jupyter server that you can open by going to [http://localhost:8888](http://localhost:8888).

## 1. Advanced data types and Schema evolution

Blog for this section is available at **[How to quickly deliver data to business users? #1. Adv Data types & Schema evolution](https://www.startdataengineering.com/post/deliver-data-quickly-with-schema-evolution-and-adv-data-types/)**.

Open JupyterLab for this section by **[clicking on this link](http://localhost:8888/doc/tree/notebooks/automate_data_flow.ipynb)**.
