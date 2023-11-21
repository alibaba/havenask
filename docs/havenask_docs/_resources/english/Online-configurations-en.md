## Overview

You can use online configurations to provide search services. You can set query parameters, such as the timeout period, query execution policy, and the method of index loading, in online configurations. Compared with offline index building, online search requires information about the index configurations. Therefore, online configurations contain some offline configuration files, such as configurations of the analyzer, clusters, and schemas. Online configurations are stored in two directories, bizs and table. The bizs directory stores configurations of online queries and the table directory stores the configurations of loaded index tables. For more information about online configuration, see [online_config](https://github.com/alibaba/havenask/tree/main/example/config/normal_config/online_config/).

## bizs

The bizs directory is the main directory for online search configurations. The subdirectory is named with numbers that indicate the version of the configurations. The program automatically loads the configuration file of the largest version number under the bizs directory. The following figure shows the directory structure:

<div align="left" >

<img src="https://user-images.githubusercontent.com/115977247/199448060-a0a24eba-5fc9-4fb1-af01-c07ba68be8f9.png" height="400" width="550" align="center" />

</div>



### zones

The subdirectory of the zones directory stores logical clusters of Havenask. When the subdirectory stores a single table, you can name the subdirectory after the table. When the subdirectory stores multiple tables, set the directory name based on your business requirements. The default_biz.json file describes the specific configurations of the cluster. For more information, see [biz configurations](https://github.com/alibaba/havenask/wiki/biz.json_en).

### qrs.json

The qrs.json file describes the configuration of the Query Result Searcher (QRS). You can use this file for native DSL queries. But this configuration file does not take effect for SQL queries and will be deprecated soon.

### schemas

For more information, see [schemas](https://github.com/alibaba/havenask/wiki/Offline-configurations-en#schemas) in offline configurations. The schemas in online configurations are the same as those in offline configurations.

### clusters

For more information, see [clusters](https://github.com/alibaba/havenask/wiki/Offline-configurations-en#schemas#clusters) in offline configurations. The clusters in online configurations are the same as those in offline configurations.

### analyzer.json

For more information, see [analyzer.json](https://github.com/alibaba/havenask/wiki/Offline-configurations-en#analyzer.json) in offline configurations. The configuration remains the same as that in offline configuration.

### plugins

The plugins directory is optional and it stores custom plug-ins used during search.

## tables

The tables directory stores the index tables loaded during online search. The subdirectory is named with numbers that indicates the version of the configuration file. The program automatically loads the configuration file of the latest version under the tables directory. The following figure shows the tables directory structure. The configurations of clusters are the same as those in offline configurations. For more information, see [clusters](https://github.com/alibaba/havenask/wiki/Offline-configurations-en#clusters).

<div align="left" >

<img src="https://user-images.githubusercontent.com/115977247/199453432-8317acf1-31a7-4dcc-ad39-439fb2533d67.png" height="200" width="300" align="center" />

</div>


