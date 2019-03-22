## Create Docker Image

### Download and Unarchive Spark

```
curl -O https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
tar xvxf spark-2.4.0-bin-hadoop2.7.tgz
```

### Copy Files

Copy the Dockerfile; log4j.properies, etc from this folder to ```spark-2.4.0-bin-hadoop2.7``` folder from above.

Copy the dependency-jars folder and jar file from target to ```spark-2.4.0-bin-hadoop2.7``` .

Something like.

```
cd spark-2.4.0-bin-hadoop2.7
cp ~/github/apache-spark-testing-mvn/docker/* .
cp -r ~/github//apache-spark-testing-mvn/target/dependency-jars .
cp -r ~/github//apache-spark-testing-mvn/target/apache-spark-testing-mvn.jar .
```

**Note:** The Dockerfile is a modified version of the file included in ```spark-2.4.0-bin-hadoop2.7\kubernetes/dockerfiles/spark/Dockerfile```


### Build and Push Docker Image

```
docker login 
docker build -t david62243/apache-spark-testing:0.1 -f kubernetes/dockerfiles/spark/Dockerfile .
docker push david62243/apache-spark-testing:0.1
```

If you've already logged in you can skip docker login.




