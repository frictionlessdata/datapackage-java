# datapackage-java

[![Build Status](https://travis-ci.org/frictionlessdata/datapackage-java.svg?branch=master)](https://travis-ci.org/frictionlessdata/datapackage-java)
[![Coverage Status](https://coveralls.io/repos/github/frictionlessdata/datapackage-java/badge.svg?branch=master)](https://coveralls.io/github/frictionlessdata/datapackage-java?branch=master)
[![License](https://img.shields.io/github/license/frictionlessdata/datapackage-java.svg)](https://github.com/frictionlessdata/datapackage-java/blob/master/LICENSE)
[![Release](https://img.shields.io/jitpack/v/github/frictionlessdata/datapackage-java)](https://jitpack.io/#frictionlessdata/datapackage-java)
[![Codebase](https://img.shields.io/badge/codebase-github-brightgreen)](https://github.com/frictionlessdata/datapackage-java)
[![Support](https://img.shields.io/badge/support-discord-brightgreen)](https://discordapp.com/invite/Sewv6av)

A Java library for working with Data Packages according to the 
[Frictionless Data](https://specs.frictionlessdata.io/data-package/) specifications. 
A Data Package is a simple container format for creating self-contained packages of data. It provides the basis 
for convenient delivery, installation and management of datasets. It shares some similarity with simple database 
formats, but lacks a robust query engine, instead focusing on exchanging bundles of related data.

Please find releases on [Jitpack](https://jitpack.io/#frictionlessdata/datapackage-java)

## Usage

### Create a Data Package

#### From JSONObject Object

```java
// Create JSON Object for testing
JSONObject jsonObject = new JSONObject("{\"name\": \"test\"}");

// Build resources.
JSONObject resource1 = new JSONObject("{\"name\": \"first-resource\", \"path\": [\"foo.txt\", \"bar.txt\", \"baz.txt\"]}");
JSONObject resource2 = new JSONObject("{\"name\": \"second-resource\", \"path\": [\"bar.txt\", \"baz.txt\"]}");

List resourceArrayList = new ArrayList();
resourceArrayList.add(resource1);
resourceArrayList.add(resource2);

JSONArray resources = new JSONArray(resourceArrayList);

// Add the resources.
jsonObject.put("resources", resources);

// Build the datapackage.
Package dp = new Package(jsonObject, true); // Set strict validation to true.
```

#### From JSON String

```java
// The path of the datapackage file.
String filepath = "/path/to/file/datapackage.json";

// Get string content version of source file.
String jsonString = new String(Files.readAllBytes(Paths.get(filepath)));

// Create DataPackage instance from jsonString.
Package dp = new Package(jsonString, true); // Set strict validation to true.
```

#### From Remote File

```java
URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
Package dp = new Package(url, true); // Set strict validation to true.
```

#### From Local JSON File

```java
String relativePath = "datapackage.json";
String basePath = "/data";
        
// Build DataPackage instance based on source file path.
Package dp = new Package(relativePath, basePath, true); // Set strict validation to true.
```

#### From Local Zip File
```java
Package dp = new Package("/path/of/zip/file/datapackage.zip", true); // Set strict validation to true.
```

Exceptions are thrown for the following scenarios:
- The zip archive must contain a file named _datapackage.json_. If no such file exists, a `DataPackageException` will be thrown.
- If _datapackage.json_ does exist but it is invalid and validation is enabled then a `ValidationException` will be thrown.
- If the zip file does not exist, an `IOException` will be thrown.


### Iterate through Data
#### Without Casting
```java
// Get the resource from the data package.
Resource resource = pkg.getResource("first-resource");

// Set the profile to tabular data resource (if it hasn't been set already).
resource.setProfile(Profile.PROFILE_TABULAR_DATA_RESOURCE);

// Get Iterator.
Iterator<String[]> iter = resource.iter();

// Iterate.
while(iter.hasNext()){
    String[] row = iter.next();
    String city = row[0];
    String population = row[1];
} 
```

#### With Casting

```
// Get Iterator. 
// Third boolean is the cast flag (First on is for keyed and second for extended).
Iterator<Object[]> iter = resource.iter(false, false, true));

// Iterator
while(iter.hasNext()){
    Object[] row = iter.next();
    String city = row[0];
    Integer population = row[1];
} 
```

### Edit a Data Package

#### Add a Resource

```java
// Create a data package.
Package dp = new Package();

// Add a resource.
Resource resource = new Resource("new-resource", "data.csv");
dp.addResource(resource);
```

A `DataPackageException` will be thrown if the name of the new resource that is being added already exists.

#### Remove a Resource

```java
// Create a data package.
URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
Package dp = new Package(url);

// Remove a resource.
dp.removeResource("third-resource");
```

#### Add a Property

```java
// Create a data package
Package dp = new Package();

// Add a few properties.
dp.addProperty("name", "a-unique-human-readable-and-url-usable-identifier");
dp.addProperty("title", "A nice title");
dp.addProperty("id", "b03ec84-77fd-4270-813b-0c698943f7ce");
dp.addProperty("profile", "tabular-data-package");

// Create and add license array.
JSONObject license = new JSONObject();
license.put("name", "ODC-PDDL-1.0");
license.put("path", "http://opendatacommons.org/licenses/pddl/");
license.put("title", "Open Data Commons Public Domain Dedication and License v1.0");

JSONArray licenses = new JSONArray();
licenses.put(license);

dp.addProperty(licenses);
```

A `DataPackageException` will be thrown if the key of the new property that is being added already exists.


#### Remove a Property

```java
// Create a data package
Package dp = new Package();

// Add a few properties.
dp.addProperty("name", "a-unique-human-readable-and-url-usable-identifier");
dp.addProperty("title", "A nice title");

// Remove the title property.
dp.removeProperty("title");
```

### Save to File

#### JSON File
```java
URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
Package dp = new Package(url);

dp.save("/destination/path/datapackage.json")
```


#### Zip File
```java
URL url = new URL("https://raw.githubusercontent.com/frictionlessdata/datapackage-java/master/src/test/resources/fixtures/multi_data_datapackage.json");
Package dp = new Package(url);

dp.save("/destination/path/datapackage.zip")
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


