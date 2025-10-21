# POSTGIS_Tools
Evolution of the 'posting' tools from 2023. The hope here is to make them a bit more flexible/modular. 

Purpose:
Access POSTGIS database and do manipulations + translations into data. 

Notes:
See local_config_EXAMPLE.py for example of configuration for local database. Create file called local_config.py using that template.

The data in the database is formatted in yearYYYY.segments_YYYY_MM format (schema.table). 

Dependancies:
- PostgreSQL Database w/ POSTGIS extension. 
- Psycopg2
- GDAL

--> I use this tool with a Python Anaconda Environment with my geographic python tools set up within there including GDAL, Psycopg2 (amongst others that aren't used here yet like GeoPandas). 
