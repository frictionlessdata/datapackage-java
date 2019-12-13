Test data files and data packages for testing Frictionless Data tooling.

These resources may also be useful to others building data tools beyond the Frictionless Data community.

# Motivation

As a Developer writing a library or tool I want to have a set of standard test data files and data packages as a reference for my implementation tests

# Contributing

Contributions are welcome! Please open a pull request.

Please keep files as small as possible (in general we are testing correctness not performance)

# Using these materials in your project

We suggest sub-moduling this repo into your own tool.

```
git submodule TODO
```

Some files may change over time, e.g. due to data package spec upgrade, but you can still keep old spec versions by submoduling from a specific branch. Below is an example `.gitmodules` file:

```
[submodule "SubmoduleTestRepo"]
    path = SubmoduleTestRepo
    url = https://github.com/frictionlessdata/test-data.git
    branch = v1.0
```

## Versioning

We plan to maintain versioned branches of this repo that track the Frictionless Data specifications so that users of this repo can test against data corresponding to specific versions of the specs.


# Data Files

We focus on tabular and geo data as these are the common forms of structured data we handle.

## Tabular Data

### CSV

* CSV structure variants
  * separators: `, ; \t`
* CSV type with all table schema types for inference
* CSV with table schema

### Non-CSV

* xls
* xlsx
* google spreadsheet

## Geo

* GeoJSON

## Other

* PDF (?)


# Table Schema


# Data Resource


# Data Packages

* datapackage.json
* README only
* Basic CSV
* Inline data
* ... 

## Tabular Data

* consider supplying Table Schemas within a datapackage.json and separately as tableschema.json as some tools take one or the other as input
* test data for each `type`, `format` and `constraint` combination
* common date time <patterns>
* missing values
* primary key
* foreign key (local or url)
* rdfType

## CSV dialect

* comma, tab, semicolon separated
* headers
* reference in-line or at url

## Data package views

## License

MIT - see LICENSE

