CREATE TABLE AnimalInfo_dim (
    animal_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    dob TIMESTAMP,
    animal_type VARCHAR,
    breed VARCHAR,
    color VARCHAR
);

CREATE TABLE Outcomes_dim (
    outcome_id VARCHAR NOT NULL PRIMARY KEY,
    outcome_type VARCHAR
);

CREATE TABLE Outcomesub_dim (
    outcomesub_id VARCHAR PRIMARY KEY,
    outcome_subtype VARCHAR
);

CREATE TABLE Outcomesex_dim (
    outcomesex_id VARCHAR PRIMARY KEY,
    outcome_sex VARCHAR
);

create Table Date_dim (
    date_id VARCHAR NOT NULL PRIMARY KEY,
    outtime TIMESTAMP,
    month VARCHAR,
    year VARCHAR
);

--include dim aor fct in the table names to indicate fact vs dimension table
CREATE TABLE AnimalRecords_fct (
    record_id VARCHAR NOT NULL PRIMARY KEY,
    animal_id VARCHAR NOT NULL REFERENCES AnimalInfo_dim(animal_id),
    date_id VARCHAR not NULL REFERENCES Date_dim(date_id),
    outcome_id VARCHAR REFERENCES Outcomes_dim(outcome_id),
    outcomesex_id VARCHAR REFERENCES Outcomesex_dim(outcomesex_id),
    outcomesub_id VARCHAR REFERENCES Outcomesub_dim(outcomesub_id)
);