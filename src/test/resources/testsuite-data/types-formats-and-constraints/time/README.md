This data package contains data for testing formats of "time" type according to specs provided here https://frictionlessdata.io/specs/table-schema/#time.

You should expect following errors when validating data against the schema in datapackage.json:

* The value "00:01" in column "default" is not type "time" and format "default"
* The value "00:01" in column "any" is not type "time" and format "any"
* The value "00/01/00" in column "pattern" is not type "time" and format "%H/%M"
