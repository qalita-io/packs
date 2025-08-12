DROP TABLE IF EXISTS iris;
CREATE TABLE iris (
  sepal_length DOUBLE,
  sepal_width DOUBLE,
  petal_length DOUBLE,
  petal_width DOUBLE,
  species VARCHAR(50)
);
LOAD DATA LOCAL INFILE '/home/aleopold/platform/tests/data/iris.csv'
INTO TABLE iris
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(sepal_length, sepal_width, petal_length, petal_width, species);
