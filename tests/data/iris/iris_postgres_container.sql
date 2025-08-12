DROP TABLE IF EXISTS iris;
CREATE TABLE iris (
  sepal_length DOUBLE PRECISION,
  sepal_width DOUBLE PRECISION,
  petal_length DOUBLE PRECISION,
  petal_width DOUBLE PRECISION,
  species TEXT
);
\copy iris(sepal_length, sepal_width, petal_length, petal_width, species) FROM '/data/iris.csv' WITH (FORMAT csv, HEADER true);
