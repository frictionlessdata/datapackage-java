package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.datapackage.resource.Resource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

public class DocumentationCases {

    @Test
    @DisplayName("Reading a Schema with a Foreign Key against non-matching data")
    void validateForeignKeyWithError() throws Exception{
        String DESCRIPTOR = "{\n" +
                "  \"name\": \"foreign-keys\",\n" +
                "  \"resources\": [\n" +
                "    {\n" +
                "      \"name\": \"teams\",\n" +
                "      \"data\": [\n" +
                "        [\"id\", \"name\", \"city\"],\n" +
                "        [\"1\", \"Arsenal\", \"London\"],\n" +
                "        [\"2\", \"Real\", \"Madrid\"],\n" +
                "        [\"3\", \"Bayern\", \"Munich\"]\n" +
                "      ],\n" +
                "      \"schema\": {\n" +
                "        \"fields\": [\n" +
                "          {\n" +
                "            \"name\": \"id\",\n" +
                "            \"type\": \"integer\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"name\",\n" +
                "            \"type\": \"string\"\n" +
                "          },\n" +
                "          {\n" +
                "            \"name\": \"city\",\n" +
                "            \"type\": \"string\"\n" +
                "          }\n" +
                "        ],\n" +
                "        \"foreignKeys\": [\n" +
                "          {\n" +
                "            \"fields\": \"city\",\n" +
                "            \"reference\": {\n" +
                "              \"resource\": \"cities\",\n" +
                "              \"fields\": \"name\"\n" +
                "            }\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"cities\",\n" +
                "      \"data\": [\n" +
                "        [\"name\", \"country\"],\n" +
                "        [\"London\", \"England\"],\n" +
                "        [\"Madrid\", \"Spain\"]\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        Package dp = new Package(DESCRIPTOR, Paths.get(""), true);
        Resource teams = dp.getResource("teams");
        DataPackageValidationException dpe = Assertions.assertThrows(DataPackageValidationException.class, () -> teams.checkRelations(dp));
        Assertions.assertEquals("Error reading data with relations: Foreign key validation failed: [city] -> [name]: 'Munich' not found in resource 'cities'.", dpe.getMessage());
    }
}
