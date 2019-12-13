This data package contains data for testing "string" formats ("default", "email" and "uri") according to specs provided here https://frictionlessdata.io/specs/table-schema/#string.

You should expect following errors when validating data against the schema in datapackage.json:

* The value "null" does not conform to the "required" constraint for column "default"
* The value "user2@domain" in column "email" is not type "string" and format "email"
* The value "https:/domain.com" in column "uri" is not type "string" and format "uri"
