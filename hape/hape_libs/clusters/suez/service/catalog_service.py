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
        
        
    def get_build(self, catalog_name, db_name, table_name):
        Logger.debug("get build task of table {}".format(table_name))
        response = requests.post(self._http_address + "/CatalogService/getBuild", json={"buildId":{"catalog_name": catalog_name, 
                                                                                         "database_name": db_name,
                                                                                         "table_name": table_name}})
        data = response.json()
        Logger.debug(str(data))
        if len(data["status"]) == 0 and 'builds' in data:
            return data["builds"][0]
        else:
            raise RuntimeError("Failed to get build, response:{}".format(data))
        
        
    
