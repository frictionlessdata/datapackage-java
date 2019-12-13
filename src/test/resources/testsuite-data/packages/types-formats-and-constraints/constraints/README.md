This datapackage contains data for testing "constraints" property according to specs provided here https://frictionlessdata.io/specs/table-schema/#datetime.

You should expect following errors when validating data against the schema in datapackage.json:

* The value "null" does not conform to the "required" constraint for column "required"
* The value "001" does not conform to the "unique" constraint for column "unique"
* The value "ab" does not conform to the "minLength" constraint for column "minLength"
* The value "abcd" does not conform to the "maxLength" constraint for column "maxLength"
* The value "2017-12-31" does not conform to the "minimum" constraint for column "minimum"
* The value "2018-02-01" does not conform to the "maximum" constraint for column "maximum"
* The value "def" does not conform to the "pattern" constraint for column "pattern"
* The value "c" does not conform to the "enum" constraint for column "enum"
