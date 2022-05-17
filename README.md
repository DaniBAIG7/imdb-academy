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
git clone https://github.com/DaniBAIG7/imdb-academy.git
```
Once cloned, we go into that folder and compose the Docker container, using the following command:
```
cd imdb-academy
docker-compose up
```
By doing that, we have the system running with the necessary versions of both the Elasticsearch and the API containers.

**IMPORTANT:**
If a new version of the system is pulled from this repository and you want the latest changes to be reflected in your container, you should do the following:
- Stop the running containers (if they are running, of course)
- Rebuild the Docker system with the following command, forcing it to not use cached information: ``` docker-compose up --build -d ```

## Data necessary for working with the API
There are several files used in the project that need to be downloaded to get it working. Those files are IMDb datasets that can be found in [this link](https://datasets.imdbws.com/); _**TBD**_

## Instructions
Firstly, you should fill Elasticsearch by indexing (at least once) all the documents on the documents mentioned earlier.
For this, a call to _http://localhost:8080/admin/api/index_documents_ is necessary (for further information regarding this endpoint, please, take a look to Swagger documentation in the last section).

A call to this endpoint will trigger the (slow, aprox. 40 min) indexing of all the data to be available in elastic. Don't worry: While indexing is working you can (under your own risk :D) start querying the database.

## Documentation
You can access a **Swagger** documentation, once the application is running, by accessing the following URL: 
http://localhost:8080/swagger-ui/index.html?urls.primaryName=imdb-public
