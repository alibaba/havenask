---
toc: content
order: 8
---

# Hape FAQ and troubleshooting

### working principle of hape
* All operations performed by hape are based on the havenask/swift/bs admin
* The admin provides a list of schedulable machines and cluster targets based on the hape to adjust the worker. hape does not directly control the worker


### havenask cluster process information
* You can run the hapes command to view the IP address and container name of the process.
* By default, the havenask cluster creates a working directory for all processes under /home/\<user\>. You can use ls ~ | grep havenask | grep \<serviceName\> on a machine. serviceName indicates the service name specified in global.conf in the cluster configuration.
* Logging:
   * All processes have basic logs:
      * Startup log :\< workdir\>/stdout.log and \<workdir\>/stdin.log
   * Main logs of the havenask admin:
      * Cluster process scheduling log \<workdir\>/hippo.log
      * Cluster target log \<workdir\>/suez.log
   * Main logs of the havenask worker:
      * SQL engine work log \<workdir\>/ha_sql/logs/sql.log
   * Main logs of swift admin:
      * Cluster process scheduling log \<workdir\>/logs/hippo.log
      * Cluster target log \<workdir\>/logs/swift/swift.log
   * Main logs of the swift broker:
      * broker work log \<workdir\>/logs/swift/swift.log
* Log on to the container:
   ```
   find <workdir> -name sshme
   ```
   ```
   bash <path-to>/sshme
   ```

* Manual process restart:
   * Enter the container first. ps -aux finds and kills the process
   * Then execute the process startup script
   ```
   find <workdir> -name process_starter.sh
   ```

   ```
   bash <path-to>/process_starter.sh &
   ```


### General troubleshooting methods

#### Hape Command Prompt ERROR Information
* Troubleshoot by using the hape debug log
   * The hape command has the-v parameter. You can open the debug-level log to see the specific commands that hape executes.

* Troubleshoot by using the hape subcommand
   * The hape validate subcommand performs basic verification on the cluster. You can use this command to identify some simple issues.
   * The hapes gs subcommand can reveal the status of clusters and tables.


#### The behavior of the havenask cluster is not as expected.
* The hapes gs subcommand checks whether the admin and worker containers are started.
* If the worker fails to start, you can check the hippo.log log of the admin to check whether the docker-related commands are successfully executed.
* If the worker has been successfully started, you can run the hapes gs command to find the cluster worker and check whether the worker log contains obvious ERROR information.
* The havenask worker can also check whether zone_config is as expected.


### FAQ
* hape validate/hape debug log /havenask admin log shows that containers and processes are abnormal
   * Generally, the corresponding container and process commands will be typed, which can be executed manually to see the reason.

* The container creation is not successful, and the docker ps --all shows that the container exits.
   * Execute the sudo chmod 666 /var/run/docker.sock on the corresponding machine



* Creating a swift topic timed out
   * This is generally caused by incorrect settings of hadoopHome and javaHome in multi-machine mode.
   * Run the hape gs swift command to check whether the broker is started successfully
   * Check the admin and broker logs for exceptions

* The qrs does not reach the target number of rows /hape gs havenask command shows some process exceptions.
   * You can check whether the corresponding log is occupied by the port.
   * You can restart a node using hape restart container commands
* The vector index does not take effect. The query results in empty data after insertion.
   * Refer to the template /ha3_install/example/cases/vector/vector_conf of the vector index example, where data_tables is different from the normal configuration