# Databricks notebook source
class DBAcademyHelper():
    def __init__(self, lesson=None, catalog="hive_metastore"):
        import re, time

        self.start = int(time.time())
        
        self.course_name = "ncouc"
        self.lesson = lesson
        self.catalog = catalog
        if self.catalog == "hive_metastore":
            self.catalogs = [ "hive_metastore" ]
        else:
            self.catalogs = [ catalog, "hive_metastore" ]

        # Define username
        self.username = spark.sql("SELECT current_user()").first()[0]
        clean_username = re.sub("[^a-zA-Z0-9]", "_", self.username)

        self.db_name_prefix = f"dbacademy_{clean_username}_{self.course_name}"
        self.source_db_name = None

        self.working_dir_prefix = f"dbfs:/user/{self.username}/dbacademy/{self.course_name}"
        
        if self.lesson:
            self.lesson = self.lesson.lower()
            clean_lesson = re.sub("[^a-zA-Z0-9]", "_", self.lesson)
            self.db_name = f"{self.db_name_prefix}_{clean_lesson}"
        else:
            self.db_name = self.db_name_prefix

    def init(self, create_db=True):
        spark.catalog.clearCache()
        self.create_db = create_db
        
        if create_db:
            for c in self.catalogs:
                print(f"\nCreating the database \"{c}.{self.db_name}\"")
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {c}.{self.db_name}")

            spark.sql(f"USE CATALOG {self.catalog}")
            spark.sql(f"USE {self.db_name}")

    def cleanup(self):
        for stream in spark.streams.active:
            print(f"Stopping the stream \"{stream.name}\"")
            stream.stop()
            try: stream.awaitTermination()
            except: pass # Bury any exceptions

        for c in self.catalogs:
            spark.sql(f"USE CATALOG {c}")
            if spark.sql(f"SHOW DATABASES").filter(f"databaseName == '{self.db_name}'").count() == 1:
                print(f"Dropping the database \"{c}.{self.db_name}\"")
                spark.sql(f"DROP DATABASE IF EXISTS {self.db_name} CASCADE")

    def conclude_setup(self):
        import time
        
        spark.conf.set("da.catalog", self.catalog)
        spark.conf.set("da.db_name", self.db_name)

        print(f"\nSetup completed in {int(time.time())-self.start} seconds")

