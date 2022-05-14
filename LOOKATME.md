# DE Challenge by Mario Leon

Hi, this is my take on DE Challenge v2 for Walmart.

I used PySpark to provide a solution to the requested information

This proyect will deploy a Jupyter Notebook container, that will submit a PySpark job on entrypint, writing the results of the analysis to the ```./output/epl_results``` folder, partitioned by season

## Requirements

Docker service running

## Code Run

You should be able to run the challenge by using the following commands, from the root of the folder:

```bash
    cd deployment/
    docker-compose up
```
