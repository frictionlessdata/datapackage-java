This datapackage contains data for testing formats of "datetime" type according to specs provided here https://frictionlessdata.io/specs/table-schema/#datetime.

You should expect following errors when validating data against the schema in datapackage.json:

* The value "2018-01-02T00:00" in column "default" is not type "datetime" and format "default"
* The value "2018/01/02T00:00:00" in column "any" is not type "datetime" and format "any"
* The value "02/01/18-00:00:00" in column "pattern" is not type "datetime" and format "%d/%m/%y-%H:%M"
