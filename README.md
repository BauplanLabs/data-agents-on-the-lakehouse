# data-agents-on-the-lakehouse
Playground for running agentic workflows over a programmable warehouse

## Overview

The flow "business question to quick analysis to scale" is notoriously difficult for humans; even before reaching production, data teams are often swamped in repetitive tasks. If data humans are slow, uninspired and perhaps not always up to date with cloud best practices, aren't data agents the right tool for the task? 

We partnered up with [Adyen](https://huggingface.co/datasets/adyen/DABstep) and [TogetherAI](https://www.together.ai/) to explore this question in the context of:

* realistic data tasks, exemplified by questions in the challenging [DABStep](https://huggingface.co/datasets/adyen/DABstep) dataset;
* open source models, with miminal-to-none task specific tuning, served from the TogetherAI platform;
* the programmable lakehouse, [Bauplan](https://www.bauplanlabs.com/), which provides concise, Pythonic APIs for data management and data transformation.

As inputs for the workflow, we have: 

* some files in S3 representing the DABStep dataset as raw data;
* the dataset markdown files describing the schemas;
* the dataset questions, which are the business questions we want to answer;
* llm-friendly information about Bauplan APIs.

Note that all of the above represents a very realistic scenario! We broke the full lifecycle into two parts, released in two separate blog posts:

* Part 1: the ETL workflow, which ingests the raw data, performs quality checks automatically, and then invokes human-in-the-loop confirmation to ensure the tables are ready and verified for the next step;
* Part 2: the analysis itself, i.e. taking the source tables, and producing working code that return the correct answer to business questions.

Please refer to the companion blog posts for the relevant background information, general architecture, and the overall goals of the project:

- Part 1: TBC, come back soon!
- Part 2: TBC, come back soon!

## Setup

### Python environment

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) then sync the environment:

```bash
uv sync
```

### API Setup

You will need to get APIs for the following services:

* [Bauplan](https://app.bauplanlabs.com/sign-up), to manage the data life cycle and run the transformation pipelines; make sure you generate a key, and [configure your own local profile](https://docs.bauplanlabs.com/en/latest/tutorial/00_installation.html);
* [TogetherAI](https://www.together.ai/), for calling open source models with great performance;
* [E2B](https://e2b.dev/), for running the code in a sandbox environment;
* (Optional) [OpenAI](https://openai.com/) - if you want to use OpenAI models instead of TogetherAI.

### Local .env file

Copy the `local.env` file to `.env` (do not commit it!) and fill in the required values, i.e. API keys for the services we need and the S3 bucket name for the raw data. 

### Raw data setup on S3

We assume you have an AWS account, in which you have created a bucket for the raw data. The bucket name should be set in the `.env` file as `S3_BUCKET_RAW_DATA`. Assuming you have your AWS credentials configured, you can run the following command to move the DABStep raw data in the S3 bucket:

```bash
cd setup
uv run prepare_cloud_setup.py
```

## Run

### Part 1: the ETL workflow

```bash
cd src/etl_agent
uv run etl_agent_loop.py
```

### Part 2

Come back soon!

## Acknowledgements
Thanks to Martin Iglesias Goyanes, Friso Kingma and the Adyen team for their fantastic dataset, and for being very open to collaboration. Thanks to Federico Bianchi and the TogetherAI team for their guidance and support in setting up proper agentic practices on top of our Pythonic lakehouse.

## License
This code in this project is released "as is" under the MIT license. All external services used in this project are subject to their own terms of service and privacy policies.