BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE iris';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/
CREATE TABLE iris (
  sepal_length NUMBER,
  sepal_width NUMBER,
  petal_length NUMBER,
  petal_width NUMBER,
  species VARCHAR2(50)
);
-- External table approach to load CSV (assumes DIRECTORY data_dir exists and is granted to app)
CREATE TABLE iris_ext (
  sepal_length NUMBER,
  sepal_width NUMBER,
  petal_length NUMBER,
  petal_width NUMBER,
  species VARCHAR2(50)
) ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY data_dir
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY NEWLINE
    SKIP 1
    FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
    (sepal_length, sepal_width, petal_length, petal_width, species)
  )
  LOCATION ('iris.csv')
)
REJECT LIMIT UNLIMITED;
INSERT INTO iris SELECT * FROM iris_ext;
