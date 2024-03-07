-- create a dummy snowflake table
CREATE
OR REPLACE TABLE snowflake (
    id INT,
    NAME STRING,
    age INT
);
-- insert some data into the table
INSERT INTO
    snowflake
VALUES
    (
        1,
        'John',
        25
    ),
    (
        2,
        'Jane',
        30
    ),
    (
        3,
        'Jim',
        35
    ),
    (
        4,
        'Jill',
        40
    ),
    (
        5,
        'Jack',
        45
    );
