# Product Index

This application demonstrates the basics of using the ElasticSearch client. After starting, the application checks if it has to create a new index. If that is the case, it reads the mapping from a file located on the classpath and programmatically creates a new index using that mapping. The application then executes a bulk index of example data that is also read from a file located on the classpath. When the index has been created, it is possible to execute search requests against it by using a simple web form.

The project uses the `docker-maven-plugin` for starting two ElasticSearch nodes and one Kibana instance. The ElasticSearch containers are dependent on Docker volumes that have to created before. Therefore, the following commands allow to control the complete lifecycle of the ElasticSearch nodes and the Kibana instance:

* Creating the Docker volumes: `mvn docker:volume-create`
* Creating and starting the Docker containers: `mvn docker:build docker:start`
* Stopping and removing the Docker containers: `mvn docker:stop`
* Removing the Docker volumes: `mvn docker:volume-remove`
