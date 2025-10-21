class DBConfig:
    DATABASE = "databasename"
    HOST = "hostname"
    PORT = 1234
    USER = "username"
    PASSWORD = "verysecretpassword"

class AreaConfig:
    clip_file = "schema.area_table"
    grid_file = "schema.grid_table"
    overlap_file = "schema.overlap_to_remove"

class IDConfig:
    segment_id = 'segmentid'
    grid_id = 'grid_id'

class directories:
    output_dir = f'path/to/output/directory'