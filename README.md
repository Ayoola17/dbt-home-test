# OakNorth DBT Home Test

## Setup Instructions

1. Install DBT using pip: `pip install dbt`
2. Set up your DBT profile using profile.yml below.
3. Clone this repository: `git clone https://github.com/ayoola17/dbt-home-test.git`

### Profile.yml settings:
I have implemented a simple sqlite database for this excerise which can be set up with the profile.yml settings below

oak_north:\
  target: dev\
  outputs:\
    dev:\
      type: sqlite\
      threads: 1\
      database: 'database'\
      schema: 'main'\
      schemas_and_paths:\
        main: 'C:\your directory\oak_north\oak_north.db'\
      schema_directory: 'C:\your directory\oak_north'

### Runing and testing
To run cd to oak_north dir in the root directory
- `dtb seed` to materialize data in seed directory in the database
- `dbt run` to run
- `dbt test` to test the test cases in the yaml file


### Code  Documentation
The staging model can be found in the models/staging directory. while the analytics model can be found in the models/marts/core directory.
The models materialize as table this was set using the dbt_project.yml 


### Test

Some rows in the source data with null values were filtered out in the models using the "where" statement. The tests in the schema files for these models typically check for null values. Rows with ambiguous years were also excluded using the "where" statement in the models to eliminate dates that seem exaggerated, which could be errors.

to run test run `dbt run`

