# Working with Foreign Keys

The library supports foreign keys described in the [Table Schema](http://specs.frictionlessdata.io/table-schema/#foreign-keys) specification. It means if your data package descriptor use `resources[].schema.foreignKeys` property for some resources a data integrity will be checked on reading operations.

Consider we have a data package:

```json
{
  "name": "foreign-keys",
  "resources": [
    {
      "name": "teams",
      "data": [
        ["id", "name", "city"],
        ["1", "Arsenal", "London"],
        ["2", "Real", "Madrid"],
        ["3", "Bayern", "Munich"]
      ],
      "schema": {
        "fields": [
          {
            "name": "id",
            "type": "integer"
          },
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "city",
            "type": "string"
          }
        ],
        "foreignKeys": [
          {
            "fields": "city",
            "reference": {
              "resource": "cities",
              "fields": "name"
            }
          }
        ]
      }
    },
    {
      "name": "cities",
      "data": [
        ["name", "country"],
        ["London", "England"],
        ["Madrid", "Spain"]
      ]
    }
  ]
}
```

Let's check relations for a `teams` resource:

````java
Package dp = new Package(DESCRIPTOR, Paths.get(""), true);
        Resource teams = dp.getResource("teams");
        DataPackageValidationException dpe 
                = Assertions.assertThrows(DataPackageValidationException.class, () -> teams.checkRelations(dp));
        Assertions.assertEquals("Error reading data with relations: Foreign key validation failed: [city] -> [name]: 'Munich' not found in resource 'cities'.", dpe.getMessage());
````
As we could see, we can read the Datapackage, but if we call `teams.checkRelations(dp)`, there is a foreign key violation. That's because our lookup table `cities` doesn't have a city of `Munich` but we have a team from there. We need to fix it in `cities` resource:

````json
{
      "name": "cities",
      "data": [
        ["name", "country"],
        ["London", "England"],
        ["Madrid", "Spain"],
        ["Munich", "Germany"]
      ]
    }
````

Now, calling `teams.checkRelations(dp)` will no longer throw an exception. 