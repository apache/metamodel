# MetaModel-as-a-service

This is a web application that allows you to access MetaModel's unified API for datastore exploration and querying - using a set of RESTful services.

## Docker building and running

```
docker build -t metamodel-service .
docker run --rm -p 8080:8080 metamodel-service
```

And then go to http://localhost:8080 (assuming localhost is your docker machine).
