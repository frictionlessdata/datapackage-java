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
import java.util.List;

/**
 * Ensure datapackages are written in a valid format and can be read back. Compare data to see it matches
 */
public class RoundtripTest {
    private static final CSVFormat csvFormat = TableDataSource
            .getDefaultCsvFormat()
            .withDelimiter('\t');

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

}
