---
toc: content
order: 5
---

# Common O&M scenarios of Hape

### hape manage multiple clusters
* All commands of hape support the optional parameter-c to specify the cluster configuration path.
* In the configuration, the domainZkRoot of global.conf and the serviceName of each cluster are used to isolate different clusters to manage multiple clusters.
* However, it should be noted that the admin, qrs and searcher ports of the current version of havenask cluster are fixed. if you want to manage multiple clusters, it is better to set different ports to prevent port conflicts.

### Update a cluster template
* The cluster_templates under Cluster Configuration represent the cluster template. User-created clusters and tables generate actual configurations based on template rendering
* After the user updates the template, the user needs to call the following subcommand. The cluster template is managed by the havenask admin and cannot be dynamically updated in the current version. Therefore, you need to call the following command. The havenask admin will restart (worker will not) and reload the latest cluster configuration.
```
### Update existing cluster configurations based on a template
hape update template
```
* The current version does not support new files in the template, but only supports the modification of existing files such as searcher_hippo.json in the template.

### Update a table schema
* The current version does not support dynamically updating existing tables. Therefore, you need to delete the table before creating the table.
```
hape delete table <options-and-arguments>
hape create table <options-and-arguments>
```

### Update table configurations
* If you only want to update the template so that it takes effect on the new table, you only need to call the
```
### Update existing cluster configurations based on a template
hape update template
```
* If you want to update the configuration of the old table, The current version does not support dynamic update of existing tables. Therefore, you need to delete tables, update templates, and then create tables. In this case, the database may not be ready. You must run the gs command to check whether the database is ready. For more information, see [Hape cluster status](./status.md).
```
hape delete table <options-and-arguments>
hape update template
hape create table <options-and-arguments>
```

```
hape gs havenask
```



### Cluster Resources&Parameter Adjustment
* The adjustment of cluster resources and parameters is essentially to modify the corresponding hippo.json in the cluster template, and restart the cluster havenask/swift
```
hape restart havenask/swift
```


### View the cluster status
* The Hape gs command is called. For more information, see [Hape cluster status](./status.md).



### Abnormal nodes in a cluster
* Some nodes may be abnormal after the cluster has been running for a period of time.
* For common exceptions that can be resolved by restarting, you can use the hape restart container command in the [Hape Command Details](./command.md) to restart a node.
* Exceptions that cannot be resolved by restarting
   * Stop the node using the hape stop container command in the [Hape command detail](./command.md)
   * Remove the IP address from the global.conf file and add a new IP address.
   * Using the hape update candidate command in the [Hape command details](./command.md), hippo scheduling will assign the processes that should belong to the current machine to the new machine after a period of time.
* In general, the hapes gs command is used to determine the havenask cluster status READY at the end.


### Modify a word splitter
* direct write table word segmentation definition
   * cluster_templates/havenask/direct_table/analyzer.json
   * cluster_templates/havenask/direct_table/biz_config/analyzer.json
* Definition of full-scale word divider:
   * cluster_templates/havenask/offline_table/analyzer.json
* You can refer to the jieba word splitter that has been implemented in the template. After modifying the corresponding word splitter definition file, call the following command
```
hape update template
```


### Load custom plug-ins
* The current version does not support dynamic update plug-ins. Users need to write built-in plug-ins and make their own images.

