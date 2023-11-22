---
toc: content
order: 1
---

# Engine configuration introduction
## Introduction
* For each table created by the user, Hape renders a table configuration and sends it to the engine based on the global and scheduling information in the [Hape configuration](hape-config.md) configured by the user
* Hape table configuration template located in hape_conf direct_tables (direct write table template) or offline_tables (full table template)


<Tree>
  <ul> 
    <li>default
      <ul>
        <ul>
        <li>global.conf</li> 
        <li>cluster_templates
            <ul>
            <li>havenask (havenask引擎进程配置)
                <ul><li>direct_tables（直写表模板）/offline_tables（全量表模板）
                </li></ul>
                <ul><li>clusters <ul><li>...</li></ul>
                </li></ul>
                <ul><li>data_tables <ul><li>...</li></ul>
                </li></ul>
                <ul><li>analyzer.json</li></ul></ul></li>
            </li>
            <li>swift
                <ul><li>...</li></ul>
            </li>
            </ul>
        </li> 
        </ul>
    </li>
    </ul>
  </ul>
</Tree>

## clusters
clusters is a mandatory directory that stores specific parameters for building one or more index tables. The configuration file is a JSON-format file. The file name consists of the index name and the suffix "_cluster.json". For details about the format of the index building configuration, see [cluster Configuration](./clusterconfig).

## data_tables
data_tables is a required directory that stores one or more table data processing configurations to guide the engine on what to do with the raw data when the index is built. The data processing configuration file is a file in json format. The file name consists of the index table name and the suffix _table.json. For details about the format of the data processing configuration file, see [data_table Configuration](./data_table.json).


## schemas
The schemas are specified when a user calls Hape to build a table, and they store the index structure configuration of one or more tables. The index configuration file is a JSON-format file. The file name consists of the index table name and the suffix _schema.json. For details about the format of the index structure configuration file, see [schema Configuration](./schemaconfig).

## analyzer.json
Analyzer. json is an analyzer configuration file that specifies all the analyzers that can be used by the engine. For details, see Lexical Configuration (./analyzer).