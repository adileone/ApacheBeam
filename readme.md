## Exercise
Create an Apache Beam pipeline (Java) that reads a local CSV file (using FileIO or TextIO) containing data related to MLB players in 2012. Also set up a table on a local SQL, which runs on a container, to load related data to MLB teams that same year.
First goal is to read as Side Input the team table from the local SQL (using JdbcIO) and make a join with the player data taken from the CSV before. The second goal is to calculate the average height of the players for each team.

The pipeline must therefore produce two outputs:

* A CSV file, containing for each row a player and the data of the team to which he belongs (result of the join).
* A CSV file, containing a team and the average height of its players for each row (result of the aggregation and the average).