# Hive experiment
In this repository is reported a simple exercise that aims to implement an ETL process on a dataset of textual data (.txt) on a Apache [Hadoop](https://hadoop.apache.org/) single node HDFS with an on-top Apache [Hive](https://hive.apache.org/) service to query the uploaded data.

### Requirements
The experiment has been realised using the following pre installed tools:

* **Poetry** : as python package manager tool
* **conda** : as python environment manager tool
* **docker** : as container manager tool



## 1) Infrastructure building
In order to build the Hadoop and Hive cluster on docker I followed the well designed tutorial [link](https://hshirodkar.medium.com/apache-hive-on-docker-4d7280ac6f8e) customising the folder creation.
Let me resume the high level commands to set-up the cluster:

> `$cd Hive `
> `$docker-compose up `

> :warning: **If you are using WLS**: run docker from Powershell in order to avoid problems in persisted volumes creation for the hive server 

Once Docker is ready, and all the containers are up and runnig we can move to run the ETL flow.

> :warning: **If you are unsing WLS**: if the cluster doesn't start because of some of a container problem let's try to type the following commands from cmd (as administrator):\
    `$net stop winnat ` \
start the cluster again and then type: \
    `$net start winnat ` \
This command could stop the internet access to the Linux system.

## 2) ETL flow running
When the infrastructure is ready let's create a python environment (using conda, virtualenv or the environment manager you prefere) and then install all the dependencies necessary to run the python scritp using Poetry, or if you prefere copy the pacakges from the poetry file and install using pip or conda.
Once the environment is ready let's download *.tar.gz* ([link](https://github.com/PDXostc/rvi_big_data/blob/master/cabspottingdata.tar.gz)) file containing all the textual data.
Once the download process is complete we can procede to create data strucutures (as SQL table) on Hive:

 >   `$docker exec -it hive-server /bin/bash`
 >   `$hive -f hive_stage/data_travels_table.hql`

(let's continue with all the tables inside the folder)
Each of the .hql file contains a path, in the hadoop hdfs, where data to read are stored. The update of the hql file acts in creating data structure and in creating the folder in the file system.

Now we are ready to launch the ETL flow running the python script:

 >   `$python data_preprocessing_and_upload.py `

Once data is ready we can see the uploaded files on the hadoop filesystem typing the following URL on the browswer:   

 >   `http://localhost:50070/explorer.html#/user/hive/warehouse/`

Data are available in a SQL-like fashion through the Hive server. It is possible to run queries on hive, using many tools:
*  Accessing the Hive server and run queries on it
*  Connecting the Hive on a db client (e.g. DBeaver) or in python (e.g. SqlAlchemy)

