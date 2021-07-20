# pyspark_pull_data_from_url



Data

Ratings: 

http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/ratings_Movies_and_TV.csv

Metadata: 

http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Movies_and_TV.json.gz

Some descriptions on the dataset can be found at http://jmcauley.ucsd.edu/data/amazon/links.html

 

Assignment

Create a data pipeline to extract data from the CSV/JSON files, and import it into a database/DFS in order to perform analysis.

Your solution must:
download the source data in the pipeline itself. 

have proper error handling/logging.

show expressive, re-usable, clean code.

handle duplicates.

It would be good if your solution could:
be able to handle the CSV/JSON files as a stream/abstract the file reading into a streaming model.

use a workflow orchestration tool to schedule the pipeline.

use docker containers.

be scalable in case new data were to flow in on a high-volume basis(10x bigger) and has to be imported at a regular basis.

describe the data store and tools used to query it - including the presentation layer.

The final result must answer the following questions:
What are the top 5 and bottom 5 movies in terms of overall average review ratings for a given month?

For a given month, what are the 5 movies whose average monthly ratings increased the most compared with the previous month?

