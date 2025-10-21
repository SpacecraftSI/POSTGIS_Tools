# segments_with_polygon_id.py

# 2025-10-20

# Take segments from postgis database and attach a id based on a polygon -> works with polygon or grid.

# This is targeted towards using with AIS Vessel Tracking data hence some of the built-in settings. Ideally I can make this a bit more modular down the road so it can work pretty flexibility.


# import
import os
from datetime import datetime
import psycopg2 as pg
from osgeo import gdal
from local_config import DBConfig, AreaConfig


# connects to the target database using local_config.py
conn = pg.connect(host=DBConfig.HOST,
                  port=DBConfig.PORT,
                  dbname=DBConfig.DATABASE,
                  user=DBConfig.USER,
                  password=DBConfig.PASSWORD
                  )


# Settings
stat_filter = True
aoi_clip = True
remove_overlaps = True

# Statistical Filters:
sog = 87
length = 10000
duration = 21600

# Area Filters:
clip_area = AreaConfig.clip_file
poly_area = AreaConfig.grid_file
remove_overlap_polygon = AreaConfig.overlap_file


# running configurations



# list of year/months tables to export
table_years = [2020]
# table_months = [1,2,3,4,5,6,7,8,9,10,11,12]
table_months = [1]
table_list = []

def main():
    print("\nStarting segments_with_polygon_id.py . . .\n")
    start_time = datetime.now()
    cursor = conn.cursor()

    table_list = create_table_list(table_years,table_months)
    print("Processing the following tables: ", table_list)

    # per table loop
    for table in table_list:
        schema, tablename = table.split(".")
        print(f"\nStarting process on {tablename} from {schema} . . .")
        temp_table = f"temp_{tablename}"

        # confirms a spatial index exists for the source table
        sql = f"CREATE INDEX IF NOT EXISTS idx_{tablename} ON {table} USING gist (geom)"
        cursor.execute(sql)
        conn.commit()

        # filter by area or statistics or both
        filter(cursor,table,temp_table)

        # remove overlaps with land polygon
        geo_overlap_filter(cursor, temp_table, remove_overlap_polygon)

        # intersect segments with polygon and assign each segment a polygon-id

    # end per table loop

    now = datetime.now()
    duration = now - start_time
    print(f"\nTotal runtime was: {duration}")


def create_table_list(years,months): # maybe move this to helpers as it'll probably get reused
    table = []

    for y in years:
        for m in months:
            table.append(f"year{y}.segments_{y}_{m:02d}")
            # schema.table
            # format yearYYYY.segments_YYYY_MM

    return table


def filter(cursor, table, temp_table):
    # Base filters (attribute-based)
    base_filter = f"""
    sogkt < {sog}
    AND lenm < {length}
    AND duration < {duration}
    """

    # Start building the SQL as an appended list
    sql_parts = [f"DROP TABLE IF EXISTS {temp_table};",
                 f"CREATE TABLE {temp_table} AS",
                 "SELECT a.*"]

    # Include clipped geometry if spatial clipping is requested
    if aoi_clip:
        sql_parts.append(", ST_Intersection(a.geom, b.geom) AS clipped_geom")
    sql_parts.append(f"FROM {table} a")
    if aoi_clip:
        sql_parts.append(f"JOIN {clip_area} b ON ST_Intersects(a.geom, b.geom)")

    # Add WHERE clause if attribute filters are requested
    if stat_filter:
        sql_parts.append(f"WHERE {base_filter} LIMIT 1000")
        # TODO TAKE OUT LIMIT AFTER TESTING!

    # Combine into final SQL
    sql = "\n".join(sql_parts)

    cursor.execute(sql)
    conn.commit()

    # recalculate length and duration of edges
    if aoi_clip:
        drop_sql = f"ALTER TABLE {temp_table} DROP COLUMN geom;"
        cursor.execute(drop_sql)
        rename_sql = f"ALTER TABLE {temp_table} RENAME COLUMN clipped_geom TO geom;"
        cursor.execute(rename_sql)
        conn.commit()

        recalc_sql = f"""UPDATE {temp_table}
                        SET duration = duration * (ST_LENGTH(geom) / lenm),
                        lenm = ST_LENGTH(geom);"""

        cursor.execute(recalc_sql)
        conn.commit()

    print(f"Statistical filter complete for: {table} and placed in {temp_table}")


def geo_overlap_filter(cursor, temp_table, remove_overlap_polygon):
    # create spatial index on the temp table to speed up intersections
    sql = f"CREATE INDEX IF NOT EXISTS idx_{temp_table} ON {temp_table} USING gist (geom)"
    cursor.execute(sql)

    delete_sql = f"""DELETE FROM {temp_table} a
                    USING {remove_overlap_polygon} b
                    WHERE ST_Intersects(a.geom, b.geom);"""
    cursor.execute(delete_sql)
    conn.commit()


def polygon_intersect(cursor, temp_table, poly_area):
    pass

if __name__ == "__main__":
    main()