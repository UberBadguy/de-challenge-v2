# DE Challenge v2 by Mario Leon

Hi, this is my take on DE Challenge v2 for Walmart.

I used PySpark to provide a solution to the requested information.

This proyect will deploy a Jupyter Notebook container, that will submit a PySpark job on entrypoint, writing the results of the team standings on each ranking to the ```./output/epl_results``` folder in csv format.

The output CSV uses the following structure:


Column Name | Description
--- | --- 
season | Season (ex. ```1819```)
team_name | Participant team name
season_rank | Placement for the season
points | Sumatory of all points won for the season
shot_goal_ratio | Shot on goal / Goal ratio
shot_goal_ratio_rank | Placement in ```Shot on goal / Goal ratio``` ranking
goals_scored | Goals scored
goals_scored_rank | Placement in ```Goals scored``` ranking
goals_received | Goals received
goals_received_rank | Placement in ```Goals received``` ranking

## Requirements

Internet connection (to build the container for the first time)

Docker service running

## Code Run

From the root of the folder, to get to the ```deployment``` folder and run the provided code:

```bash
cd deployment/
docker-compose up
```

## Tests

Tests are implemented via ```pytest```, and ran with the command:

```bash
pytest
```