**What are possible solutions to the failure in running the start_demo_searcher.py test script?**

* Delete the working directory example/scripts/workdir.

* If multiple Havenask containers are started, try to shut them all down and restart a new container.

* Rebuild indexes by using the script and perform queries







**What is the structure of Docker WORKDIR?**

* Working directory: example/scripts/workdir (denoted as \<workdir\> hereinafter)

* Index build logs: \<workdir\>/bs.log

* Index directory: \<workdir\>/runtimedata

* Working directory of Havenask: \<workdir\>/local_search_12000

* Havenask searcher logs: \<workdir\>/local_search_12000/in0_0/logs/ha3.log

* Havenask QRS logs: \<workdir\>/local_search_12000/qrs/logs/ha3.log
