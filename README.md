# Process documentation for Sparkify data warehouse in amazon web services

In this project a data warehouse for sparkify is set up. 

## Implementation
The ETL.ipynb is the jupiter workbook from where you can execute the code to set up the data warehouse.
### Prerequisites

Before getting started you need a instance of python, either JupiterNotebook or for example a local anaconda distribution.
In both cases the following packages need to be installed:

```
import os
import glob
import configparser
import psycopg2
import pandas as pd
```
The follwing files are provided by this project:
```
import create_tables as ct
import etl as et
import setDB as sDB
```
The setDB provides the functiosn to create the roles, set up the iam role and the cluster.
The create_tables provides the fucntions to create the tables for staging and the schema.
The etl provides the functions to insert the schema.

### Running

In the etl.ipynb the code is described step by step. 

First the aws credentials need to be insert into the dwh.cfg in the cluster [AWS].

Afterwards the warehouse and the cluster is set up, as this project does not assume it is set up beforehand.

The next step is that the config file condwh is saved where the entrypoint to the database is saved. 

The last step is the set up of tables.

```
ct.main()
```

After that the ETL-process is done.

```
et.main()
```

At the end the whole ETL-process can be started for all files with the following command:

```
et.main()
```


### Important!!!
At the end dont forget to use the code provided to delete the cluster and roles.


## Authors

* **Simon Unterbusch** - *Initial work* 