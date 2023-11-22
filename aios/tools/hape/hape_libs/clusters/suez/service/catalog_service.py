import sys
import os
import requests
import json
import time

from hape_libs.utils.logger import Logger
from aios.suez.python.catalog_builder import *


class CatalogService(object):
    def __init__(self, address):
        self._http_address = address
        Logger.info("Visit catalog service in address {}".format(self._http_address))

    def create_catalog(self, catalog_json):
        Logger.debug("create catalog request {}".format(catalog_json))
        response = requests.post(self._http_address + "/CatalogService/createCatalog", json={"catalog": catalog_json})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to create catalog, response:{}".format(data))

    def get_catalog(self, catalog_name):
        response = requests.post(self._http_address + "/CatalogService/getCatalog", json={"catalog_name": catalog_name})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0:
            return data["catalog"]
        else:
            raise RuntimeError("Failed to get catalog, response:{}".format(data))

    def create_database(self, catalog_name, database_json):
        Logger.debug("create database request {}".format(database_json))
        response = requests.post(self._http_address + "/CatalogService/createDatabase", json={"database": database_json, "catalog_version": self.get_catalog_version(catalog_name)})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to create catalog, response:{}".format(data))

    def delete_database(self, catalog_name, db_name):
        Logger.debug("delete database {}".format(db_name))
        response = requests.post(self._http_address + "/CatalogService/dropDatabase", json={"catalog_name": catalog_name,
                                                                                         "database_name": db_name,
                                                                                         "catalog_version": self.get_catalog_version(catalog_name)})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0:
            return data
        else:
            raise RuntimeError("Failed to delete catalog, response:{}".format(data))


    def get_database(self, catalog_name, db_name):
        Logger.debug("get database {}".format(db_name))
        response = requests.post(self._http_address + "/CatalogService/getDatabase", json={"catalog_name": catalog_name,
                                                                                         "database_name": db_name,
                                                                                         "catalog_version": self.get_catalog_version(catalog_name)})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0:
            return data['database']
        else:
            raise RuntimeError("Failed to get database, response:{}".format(data))

    def get_table(self, catalog_name, db_name, table_name):
        Logger.debug("get table {}".format(table_name))
        response = requests.post(self._http_address + "/CatalogService/getTable", json={"catalog_name": catalog_name,
                                                                                         "database_name": db_name,
                                                                                         "table_name": table_name,
                                                                                         "catalog_version": self.get_catalog_version(catalog_name)})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0:
            return data['table']
        else:
            raise RuntimeError("Failed to get table, response:{}".format(data))


    def list_table(self, catalog_name, db_name):
        Logger.debug("get table list")
        response = requests.post(self._http_address + "/CatalogService/listTable", json={"catalog_name": catalog_name,
                                                                                         "database_name": db_name,
                                                                                         "catalog_version": self.get_catalog_version(catalog_name)})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0:
            return data.get('tableNames', [])
        else:
            raise RuntimeError("Failed to list tables, response:{}".format(data))


    def get_catalog_version(self, catalog_name):
        response = self.get_catalog(catalog_name)
        return response["version"]


    def create_table(self, catalog_name, table_json):
        Logger.debug("create table request {}".format(json.dumps(table_json)))
        response = requests.post(self._http_address + "/CatalogService/createTable", json={"table": table_json,
                                                                                           "catalog_version": self.get_catalog_version(catalog_name)})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to create table, response:{}".format(data))

    def update_table_structure(self, catalog_name, table_structure_json):
        Logger.debug("update table structure request {}".format(json.dumps(table_structure_json)))
        response = requests.post(self._http_address + "/CatalogService/updateTableStructure",
                                 json={
                                     "table_structure": table_structure_json,
                                     "catalog_version": self.get_catalog_version(catalog_name)
                                 })
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to update table structure, response:{}".format(data))

    def update_partition_table_structure(self, catalog_name, database_name, table_name, partition_name):
        update_partition_table_structure_request = {
            "catalog_version": self.get_catalog_version(catalog_name),
            "catalog_name": catalog_name,
            "database_name": database_name,
            "table_name": table_name,
            "partition_name": partition_name,
        }
        Logger.debug("update partition table structure request {}".format(
            json.dumps(update_partition_table_structure_request)))
        response = requests.post(self._http_address + "/CatalogService/updatePartitionTableStructure",
                                 json=update_partition_table_structure_request)
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to update partition table structure, response:{}".format(data))

    def update_partition(self, catalog_name, partition_json):
        update_partition_request = {
            "catalog_version": self.get_catalog_version(catalog_name),
            "partition": partition_json,
        }
        Logger.debug("update partition request {}".format(
            json.dumps(update_partition_request)))
        response = requests.post(self._http_address + "/CatalogService/updatePartition",
                                 json=update_partition_request)
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to update partition, response:{}".format(data))

    def delete_table(self, catalog_name, db_name, table_name):
        Logger.debug("delete table catalog:{} database:{} table:{}".format(catalog_name,db_name, table_name))
        response = requests.post(self._http_address + "/CatalogService/dropTable", json={"table_name": table_name,
                                                                                           "catalog_name": catalog_name,
                                                                                           "database_name": db_name,
                                                                                           "catalog_version": self.get_catalog_version(catalog_name)})
        data = response.json()
        if len(data["status"]) == 0:
            return data
        else:
            raise RuntimeError("Failed to delete table, response:{}".format(data))

    def create_load_strategy(self, catalog_name, load_strategy_json):
        Logger.debug("create load strategy:{}".format(load_strategy_json))
        response = requests.post(self._http_address + "/CatalogService/createLoadStrategy",
                                 json={
                                     "catalog_version": self.get_catalog_version(catalog_name),
                                     "load_strategy": load_strategy_json
                                 })
        data = response.json()
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to create load strategy, response:{}".format(data))

    def update_load_strategy(self, catalog_name, load_strategy_json):
        Logger.debug("update load strategy:{}".format(load_strategy_json))
        response = requests.post(self._http_address + "/CatalogService/updateLoadStrategy",
                                 json={
                                     "catalog_version": self.get_catalog_version(catalog_name),
                                     "load_strategy": load_strategy_json
                                 })
        data = response.json()
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data
        else:
            raise RuntimeError("Failed to update load strategy, response:{}".format(data))


    def list_load_strategy(self, catalog_name, database_name, table_group_name):
        Logger.debug("list load strategy: catalog_name:{} database_name: {} table_group_name:{}"
                     .format(catalog_name, database_name, table_group_name))
        response = requests.post(self._http_address + "/CatalogService/listLoadStrategy",
                                 json={
                                     "catalog_version": self.get_catalog_version(catalog_name),
                                     "catalog_name": catalog_name,
                                     "database_name": database_name,
                                     "table_group_name": table_group_name
                                 })
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 or data['status']['code'] == "DUPLICATE_ENTITY":
            return data.get("tableNames", [])
        else:
            raise RuntimeError("Failed to list load strategy, response:{}".format(data))

    def get_build(self, catalog_name, db_name, table_name):
        Logger.debug("get build task of table {}".format(table_name))
        response = requests.post(self._http_address + "/CatalogService/getBuild", json={"buildId":{"catalog_name": catalog_name,
                                                                                         "database_name": db_name,
                                                                                         "table_name": table_name}})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 and 'builds' in data:
            return data["builds"]
        else:
            raise RuntimeError("Failed to get build, response:{}".format(data))

    def drop_build(self, catalog_name, db_name, table_name, partition_name, generation_id):
        Logger.debug("drop build task of table {}".format(table_name))
        response = requests.post(self._http_address + "/CatalogService/dropBuild",
                                 json={"buildId":{"catalog_name": catalog_name,
                                                  "database_name": db_name,
                                                  "table_name": table_name,
                                                  "partition_name": partition_name,
                                                  "generation_id": generation_id
                                                  }})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0:
            return data
        else:
            raise RuntimeError("Failed to drop build, response:{}".format(data))
