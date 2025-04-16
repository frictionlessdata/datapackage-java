package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.TestUtil;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.frictionlessdata.datapackage.Profile.PROFILE_TABULAR_DATA_RESOURCE;

/**
 * Ensure datapackages are written in a valid format and can be read back. Compare data to see it matches
 */
public class RoundtripTest {
    private static final CSVFormat csvFormat = TableDataSource
            .getDefaultCsvFormat().builder().setDelimiter('\t').get();

    @Test
    @DisplayName("Roundtrip test - write datapackage, read again and compare data")
    // this lead to the discovery of https://github.com/frictionlessdata/specs/issues/666, just in case
    // we see regressions in the specs somehow
    public void dogfoodingTest() throws Exception {
        // create a new Package, set Schema and data and write out to temp storage
        List<Resource> resources = new ArrayList<>();
        Package pkg = new Package(resources);

        JSONDataResource res = new JSONDataResource("population", resourceContent);
        //set a schema to guarantee the ordering of properties
        Schema schema = Schema.fromJson(
                new File(TestUtil.getBasePath().toFile(), "/schema/population_schema.json"), true);
        res.setSchema(schema);
        res.setShouldSerializeToFile(true);
        res.setSerializationFormat(Resource.FORMAT_CSV);
        res.setDialect(Dialect.fromCsvFormat(csvFormat));
        pkg.addResource(res);

        Path tempDirPath = Files.createTempDirectory("datapackage-");
        File createdFile = new File(tempDirPath.toFile(), "test_save_datapackage.zip");
        pkg.write(createdFile, true);

        // create new Package from the serialized form and check they are equal
        Package testPkg = new Package(createdFile.toPath(), true);
        Assertions.assertEquals(1, testPkg.getResources().size());

        Resource testRes = testPkg.getResource("population");
        List testData = testRes.getData(false, true, false, false);
        Resource validationRes = pkg.getResource("population");
        List validationData = validationRes.getData(false, true, false, false);
        Assertions.assertEquals(validationData.size(), testData.size());

        for (int i = 0; i < validationData.size(); i++) {
            Assertions.assertArrayEquals(((Object[])validationData.get(i)), ((Object[])testData.get(i)));

        }
    }

    @Test
    @DisplayName("Roundtrip resource")
    void validateResourceRoundtrip() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/roundtrip/datapackage.json");
        Package dp = new Package(resourcePath, true);
        Resource referenceResource = dp.getResource("test2");
        List<Object[]> referenceData = referenceResource.getData(false, false, true, false);
        String data = createCSV(referenceResource.getHeaders(), referenceData);
        Resource newResource = new CSVDataResource(referenceResource.getName(),data);
        newResource.setDescription(referenceResource.getDescription());
        newResource.setSchema(referenceResource.getSchema());
        newResource.setSerializationFormat(Resource.FORMAT_CSV);
        List testData = newResource.getData(false, false, true, false);
        Assertions.assertArrayEquals(referenceData.toArray(), testData.toArray());
    }

    @Test
    @DisplayName("Create data resource, compare descriptor")
    void validateCreateResourceDescriptorRoundtrip() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/roundtrip/datapackage.json");
        Package pkg = new Package(resourcePath, true);
        JSONDataResource resource = new JSONDataResource("test3", resourceContent);
        resource.setSchema(Schema.fromJson(
                new File(TestUtil.getBasePath().toFile(), "/schema/population_schema.json"), true));

        resource.setShouldSerializeToFile(false);
        resource.setSerializationFormat(Resource.FORMAT_CSV);
        resource.setDialect(Dialect.fromCsvFormat(csvFormat));
        resource.setProfile(PROFILE_TABULAR_DATA_RESOURCE);
        resource.setEncoding("utf-8");
        pkg.addResource(resource);
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        File createdFile = new File(tempDirPath.toFile(), "test_save_datapackage.zip");
        pkg.write(createdFile, true);
        System.out.println(tempDirPath);

        // create new Package from the serialized form and check they are equal
        Package testPkg = new Package(createdFile.toPath(), true);
        String json = testPkg.asJson();
        Assertions.assertEquals(
                descriptorContentInlined.replaceAll("[\n\r]+", "\n"),
                json.replaceAll("[\n\r]+", "\n")
        );
    }


    @Test
    @DisplayName("Create data resource and make it write to file, compare descriptor")
    void validateCreateResourceDescriptorRoundtrip2() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/roundtrip/datapackage.json");
        Package pkg = new Package(resourcePath, true);
        JSONDataResource resource = new JSONDataResource("test3", resourceContent);
        resource.setSchema(Schema.fromJson(
                new File(TestUtil.getBasePath().toFile(), "/schema/population_schema.json"), true));

        resource.setShouldSerializeToFile(true);
        resource.setSerializationFormat(Resource.FORMAT_CSV);
        resource.setDialect(Dialect.fromCsvFormat(csvFormat));
        resource.setProfile(PROFILE_TABULAR_DATA_RESOURCE);
        resource.setEncoding("utf-8");
        pkg.addResource(resource);
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        File createdFile = new File(tempDirPath.toFile(), "test_save_datapackage.zip");
        pkg.write(createdFile, true);

        // create new Package from the serialized form and check they are equal
        Package testPkg = new Package(createdFile.toPath(), true);
        String json = testPkg.asJson();
        Assertions.assertEquals(
                descriptorContent.replaceAll("[\n\r]+", "\n"),
                json.replaceAll("[\n\r]+", "\n")
        );
    }

    private static String createCSV(String[] headers, List<Object[]> data) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(",", Arrays.asList(headers)));
        sb.append("\n");
        for (Object[] row : data) {
            sb.append(Arrays.stream(row)
                    .map((o)-> (o == null) ? "" : o.toString())
                    .collect(Collectors.joining(",")));
            sb.append("\n");
        }
        return sb.toString();
    }


    private static final String resourceContent = "[\n" +
            "    {\n" +
            "\t  \"city\": \"london\",\n" +
            "\t  \"year\": 2017,\n" +
            "\t  \"population\": 8780000\n" +
            "\t},\n" +
            "\t{\n" +
            "\t  \"city\": \"paris\",\n" +
            "\t  \"year\": 2017,\n" +
            "\t  \"population\": 2240000\n" +
            "\t},\n" +
            "\t{\n" +
            "\t  \"city\": \"rome\",\n" +
            "\t  \"year\": 2017,\n" +
            "\t  \"population\": 2860000\n" +
            "\t}\n" +
            "  ]";

    private static final String descriptorContent = "{\n" +
            "  \"name\" : \"foreign-keys\",\n" +
            "  \"profile\" : \"data-package\",\n" +
            "  \"resources\" : [ {\n" +
            "    \"name\" : \"test2\",\n" +
            "    \"profile\" : \"data-resource\",\n" +
            "    \"schema\" : \"schema/test2.json\",\n" +
            "    \"path\" : \"data/test2.csv\"\n" +
            "  }, {\n" +
            "    \"name\" : \"test3\",\n" +
            "    \"profile\" : \"tabular-data-resource\",\n" +
            "    \"encoding\" : \"utf-8\",\n" +
            "    \"dialect\" : \"dialect/test3.json\",\n" +
            "    \"schema\" : \"schema/population_schema.json\",\n" +
            "    \"path\" : \"data/test3.csv\"\n" +
            "  } ]\n" +
            "}";


    private static String descriptorContentInlined ="{\n" +
            "  \"name\" : \"foreign-keys\",\n" +
            "  \"profile\" : \"data-package\",\n" +
            "  \"resources\" : [ {\n" +
            "    \"name\" : \"test2\",\n" +
            "    \"profile\" : \"data-resource\",\n" +
            "    \"schema\" : \"schema/test2.json\",\n" +
            "    \"path\" : \"data/test2.csv\"\n" +
            "  }, {\n" +
            "    \"name\" : \"test3\",\n" +
            "    \"profile\" : \"tabular-data-resource\",\n" +
            "    \"encoding\" : \"utf-8\",\n" +
            "    \"format\" : \"json\",\n" +
            "    \"dialect\" : {\n" +
            "      \"caseSensitiveHeader\" : false,\n" +
            "      \"quoteChar\" : \"\\\"\",\n" +
            "      \"doubleQuote\" : true,\n" +
            "      \"delimiter\" : \"\\t\",\n" +
            "      \"lineTerminator\" : \"\\r\\n\",\n" +
            "      \"nullSequence\" : \"\",\n" +
            "      \"header\" : true,\n" +
            "      \"csvddfVersion\" : 1.2,\n" +
            "      \"skipInitialSpace\" : true\n" +
            "    },\n" +
            "    \"schema\" : {\n" +
            "      \"fields\" : [ {\n" +
            "        \"name\" : \"city\",\n" +
            "        \"title\" : \"city\",\n" +
            "        \"type\" : \"string\",\n" +
            "        \"format\" : \"default\",\n" +
            "        \"description\" : \"The city.\"\n" +
            "      }, {\n" +
            "        \"name\" : \"year\",\n" +
            "        \"title\" : \"year\",\n" +
            "        \"type\" : \"year\",\n" +
            "        \"format\" : \"default\",\n" +
            "        \"description\" : \"The year.\"\n" +
            "      }, {\n" +
            "        \"name\" : \"population\",\n" +
            "        \"title\" : \"population\",\n" +
            "        \"type\" : \"integer\",\n" +
            "        \"format\" : \"default\",\n" +
            "        \"description\" : \"The population.\"\n" +
            "      } ]\n" +
            "    },\n" +
            "    \"data\" : [ {\n" +
            "      \"city\" : \"london\",\n" +
            "      \"year\" : 2017,\n" +
            "      \"population\" : 8780000\n" +
            "    }, {\n" +
            "      \"city\" : \"paris\",\n" +
            "      \"year\" : 2017,\n" +
            "      \"population\" : 2240000\n" +
            "    }, {\n" +
            "      \"city\" : \"rome\",\n" +
            "      \"year\" : 2017,\n" +
            "      \"population\" : 2860000\n" +
            "    } ]\n" +
            "  } ]\n" +
            "}";
}
