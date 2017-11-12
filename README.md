# datapackage-java
[![Build Status](https://travis-ci.org/frictionlessdata/datapackage-java.svg?branch=master)](https://travis-ci.org/frictionlessdata/datapackage-java)
[![Coverage Status](https://coveralls.io/repos/github/frictionlessdata/datapackage-java/badge.svg?branch=master)](https://coveralls.io/github/frictionlessdata/datapackage-java?branch=master)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)

A Java library for working with Data Package.

## Usage

### Data Package

#### From JSONObject Object

```java
// Create JSON Object for testing
JSONObject jsonObject = new JSONObject("{\"name\": \"test\"}");

// Build resources
JSONObject resource1 = new JSONObject("{\"name\": \"first-resource\", \"path\": [\"foo.txt\", \"bar.txt\", \"baz.txt\"]}");
JSONObject resource2 = new JSONObject("{\"name\": \"second-resource\", \"path\": [\"bar.txt\", \"baz.txt\"]}");

List resourceArrayList = new ArrayList();
resourceArrayList.add(resource1);
resourceArrayList.add(resource2);

JSONArray resources = new JSONArray(resourceArrayList);

// Add the resources
jsonObject.put("resources", resources);

// Build the datapackage
Package dp = new Package(jsonObject, true); // Set strict validation to true.
```

#### From JSON String

```java
// The path of the datapackage file:
String filepath = "/path/to/file/datapackage.json";

// Get string content version of source file.
String jsonString = new String(Files.readAllBytes(Paths.get(filepath)));

// Create DataPackage instance from jsonString
Package dp = new Package(jsonString, true); // Set strict validation to true.
```

#### From Remote File

```java
URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
Package dp = new Package(url, true); // Set strict validation to true.
```

#### From Local File

```java
String relativePath = "datapackage.json";
String basePath = "/data";
        
// Build DataPackage instance based on source file path.
Package dp = new Package(relativePath, basePath, true); // Set strict validation to true.
```

## Contributing

Found a problem and would like to fix it? Have that great idea and would love to see it in the repository?

> Please open an issue before you start working.

It could save a lot of time for everyone and we are super happy to answer questions and help you along the way. Furthermore, feel free to join [frictionlessdata Gitter chat room](https://gitter.im/frictionlessdata/chat) and ask questions.

This project follows the [Open Knowledge International coding standards](https://github.com/okfn/coding-standards).

Get started:
```sh
# install jabba and maven2
$ cd tableschema-java
$ jabba install 1.8
$ jabba use 1.8
$ mvn install -DskipTests=true -Dmaven.javadoc.skip=true -B -V
$ mvn test -B
```

Make sure all tests pass.


