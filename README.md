
<center><img src="https://raw.githubusercontent.com/colav/colav.github.io/master/img/Logo.png"/></center>

# OpenAlex Colombia Load
A set of scripts and documentation to download openalex, uncompress it, load it to mongodb and to get the colombian works.


# Installation

For Amazon Web Services
https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html


`pip install joblib`


Install mongodb
https://www.mongodb.com/docs/v7.0/administration/install-community/

Download openalex data, following the next instructions
https://docs.openalex.org/download-all-data/download-to-your-machine


## Using parallel uncompress

Put the script in the openalex-snapshot folder and run 
`python uncompress.py`

it takes about 2 hours using 20 jobs and a high speed nvme disk
and it requires about 2.5T of storage uncompressed.


## Usiing parallel load

Put the script in the openalex-snapshot folder and run 
`python load.py`

it takes about 300 min using 20 jobs and a high speed nvme disk
and it requires about 1T of storage in mongodb.

## Usiing colombia_cut.py

please edit in the script de db_in (input openalex) db_out (output results)
`python colombia_cut.py`

this takes a lot of time, please be patient


# License
BSD-3-Clause License 

# Links
http://colav.udea.edu.co/



