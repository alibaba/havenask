# Overview
You can use offline configurations to build indexes. You can set the structure of the index table, method of data processing, parameters for index building, and analyzer plug-ins. The directory for offline configurations must be a number. When you build an index, you must specify the upper-level directory of the configuration directory. The index building program automatically uses the latest configurations in the directory to build the index. The following example shows an offline configuration directory. You can also refer to [offline_config](https://github.com/alibaba/havenask/tree/main/example/config/normal_config/offline_config/).

<div align="left" >
<img src="https://user-images.githubusercontent.com/115977247/199416526-176caadf-0c1b-4bc1-be29-0d4f572ffb68.png" height="350" width="550" align="center" />
</div>

# schemas
The schemas directory is required. This directory stores the index schemas of one or more tables. The index schema configuration file is a JSON file. The name of the file consists of the name of the index table and suffix "_schema.json". For more information about the format of the index schema configuration file, see [Configure the schema ](https://github.com/alibaba/havenask/wiki/schema-config-en).

# data_tables
The data_tables directory is required. This directory stores data processing configurations of one or more tables. The engine can process raw data based on the configurations in this directory when you build indexes. The data processing configuration file is a JSON file. The name of the file consists of the name of the index table and suffix "_table.json". For more information about the format of the data processing configuration file, see [Configure the data table](https://github.com/alibaba/havenask/wiki/data_table.json-en).

# clusters
The clusters directory is required. This directory stores the specific parameters of one or more index tables. The cluster configuration file is a JSON file. The name of the file consists of the name of the index table and suffix "_cluster.json". For more information about the format of the cluster configuration file, see [Configure the cluster](https://github.com/alibaba/havenask/wiki/cluster-config-en).

# plugins
The plugins directory is optional. This directory stores custom plug-ins used when you build indexes.

# analyzer.json
analyzer.json is an analyzer configuration file that specifies all analyzers available for the engine. For more information about the format, see [Configure the analyzer](https://github.com/alibaba/havenask/wiki/analyzer-en).

