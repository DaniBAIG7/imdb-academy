# imdb-academy
This project is currently an app to learn how to use ElasticSearch and connect it with Java, allowing to index a huge number of documents.

## Technologies and versions
The IMDb academy first version has been written using the following:
- Java 17.0.2
- Docker
- SpringBoot 2.6.6
- Apache Maven 3.8.5
- ElasticSearch 7.16.2

## Installing and running
To run the application, you should first download the code by cloning the repo, using the following command:
```
https://github.com/DaniBAIG7/imdb-academy.git
```
Then, you should correctly build the project using Maven
```
mvn compile
```
Just after that, the Docker container with the ElasticSearch image should be ran. For this you have to download the ElasticSearch 7.16.2 image first. You can do both things by running the next commands:
```
docker pull docker.elastic.co/elasticsearch/elasticsearch:7.16.2
docker run -d --name elasticsearch-imdb -p 9200:9200 -p 9300:9300 \
 -e "discovery.type=single-node" \
docker.elastic.co/elasticsearch/elasticsearch:7.16.2
```
By doing that, we have the container running with the necessary version of ElasticSearch.

There are several files used in the project that need to be downloaded to get it working. Those files are IMDb datasets that can be found in (this link)[https://datasets.imdbws.com/];_title\_basics.tsv_ and _title\_ratings.tsv_.
After downloading them, they must be placed in the project folder **src/main/resources/static**.

Finally, the last thing remaining is to launch the project using Maven, for which we use the command
```
mvn spring-boot:run
