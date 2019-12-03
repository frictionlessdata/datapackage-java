This datapackage contains data for testing formats of "date" type according to specs provided here https://frictionlessdata.io/specs/table-schema/#date.

You should expect following errors when validating data against the schema in datapackage.json:

* The value "2018-01" in column "default" is not type "date" and format "default"
* The value "01.01.18" in column "pattern" is not type "date" and format "%d/%m/%y"
