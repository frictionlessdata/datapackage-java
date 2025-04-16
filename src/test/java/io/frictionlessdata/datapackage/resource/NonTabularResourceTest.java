package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.*;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.exception.TypeInferringException;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.frictionlessdata.datapackage.Profile.PROFILE_DATA_PACKAGE_DEFAULT;
import static io.frictionlessdata.datapackage.Profile.PROFILE_DATA_RESOURCE_DEFAULT;

public class NonTabularResourceTest {

    private static final String REFERENCE1 = "{\n" +
            "  \"name\" : \"employees\",\n" +
            "  \"profile\" : \"data-package\",\n" +
            "  \"resources\" : [ {\n" +
            "    \"name\" : \"employee-data\",\n" +
            "    \"profile\" : \"tabular-data-resource\",\n" +
            "    \"schema\" : \"schema.json\",\n" +
            "    \"path\" : \"data/employees.csv\"\n" +
            "  }, {\n" +
            "    \"name\" : \"non-tabular-resource\",\n" +
            "    \"profile\" : \"data-resource\",\n" +
            "    \"format\" : \"pdf\",\n" +
            "    \"path\" : \"data/sample.pdf\"\n" +
            "  } ]\n" +
            "}";

    private static final String REFERENCE2 = "{\n" +
            "  \"name\" : \"employees\",\n" +
            "  \"profile\" : \"data-package\",\n" +
            "  \"resources\" : [ {\n" +
            "    \"name\" : \"employee-data\",\n" +
            "    \"profile\" : \"tabular-data-resource\",\n" +
            "    \"schema\" : \"schema.json\",\n" +
            "    \"path\" : \"data/employees.csv\"\n" +
            "  }, {\n" +
            "    \"name\" : \"non-tabular-resource\",\n" +
            "    \"profile\" : \"data-resource\",\n" +
            "    \"format\" : \"json\",\n" +
            "    \"data\" : {\n" +
            "      \"name\" : \"John Doe\",\n" +
            "      \"age\" : 30\n" +
            "    }\n" +
            "  } ]\n" +
            "}";

    @Test
    @DisplayName("Test adding a non-tabular resource, and saving package")
    public void testNonTabularResource1() throws Exception {
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        String fName = "/fixtures/datapackages/employees/datapackage.json";
        Path resourcePath = TestUtil.getResourcePath(fName);
        io.frictionlessdata.datapackage.Package dp = new Package(resourcePath, true);
        dp.setProfile(PROFILE_DATA_PACKAGE_DEFAULT);

        byte[] referenceData = TestUtil.getResourceContent("/fixtures/files/sample.pdf");
        Resource<?> testResource = new myNonTabularResource(referenceData, Path.of("data/sample.pdf"));
        testResource.setShouldSerializeToFile(true);
        dp.addResource(testResource);

        // write the package with the new resource to the file system
        File outFile = new File (tempDirPath.toFile(), "datapackage.json");
        dp.write(outFile, false);

        // read back the datapackage.json and compare to the expected output
        String content = String.join("\n", Files.readAllLines(outFile.toPath()));
        Assertions.assertEquals(REFERENCE1.replaceAll("[\n\r]+", "\n"), content.replaceAll("[\n\r]+", "\n"));

        // read the package back in and check number of resourcces
        Package round2Package = new Package(outFile.toPath(), true);
        Assertions.assertEquals(2, round2Package.getResources().size());

        // compare the non-tabular resource with expected values
        Resource<?> round2Resource = round2Package.getResource("non-tabular-resource");
        Assertions.assertEquals("data-resource", round2Resource.getProfile());
        Assertions.assertArrayEquals(referenceData, (byte[]) round2Resource.getRawData());

        // write the package out again
        Path tempDirPathRound3 = Files.createTempDirectory("datapackage-");
        File outFileRound3 = new File (tempDirPathRound3.toFile(), "datapackage.json");
        round2Package.write(outFileRound3, false);

        // read back the datapackage.json and compare to the expected output
        String contentRound3 = String.join("\n", Files.readAllLines(outFileRound3.toPath()));
        Assertions.assertEquals(REFERENCE1.replaceAll("[\n\r]+", "\n"), contentRound3.replaceAll("[\n\r]+", "\n"));

        // read the package back in and check number of resourcces
        Package round3Package = new Package(outFileRound3.toPath(), true);
        Assertions.assertEquals(2, round3Package.getResources().size());

        // compare the non-tabular resource with expected values
        Resource<?> round3Resource = round3Package.getResource("non-tabular-resource");
        Assertions.assertEquals("data-resource", round3Resource.getProfile());
        Object rawData = round3Resource.getRawData();
        Assertions.assertArrayEquals(referenceData, (byte[]) rawData);
    }


    @Test
    @DisplayName("Test adding a non-tabular JSON resource, and saving package")
    public void testNonTabularResource2() throws Exception {
        Path tempDirPath = Files.createTempDirectory("datapackage-");
        String fName = "/fixtures/datapackages/employees/datapackage.json";
        Path resourcePath = TestUtil.getResourcePath(fName);
        io.frictionlessdata.datapackage.Package dp = new Package(resourcePath, true);
        dp.setProfile(PROFILE_DATA_PACKAGE_DEFAULT);

        ObjectNode referenceData = (ObjectNode) JsonUtil.getInstance().createNode("{\"name\": \"John Doe\", \"age\": 30}");
        Resource<?> testResource = new JSONObjectResource("non-tabular-resource", referenceData);
        dp.addResource(testResource);

        File outFile = new File (tempDirPath.toFile(), "datapackage.json");
        dp.write(outFile, false);

        String content = String.join("\n", Files.readAllLines(outFile.toPath()));
        Assertions.assertEquals(REFERENCE2.replaceAll("[\n\r]+", "\n"), content.replaceAll("[\n\r]+", "\n"));

        Package round2Package = new Package(outFile.toPath(), true);
        Assertions.assertEquals(2, round2Package.getResources().size());
        Assertions.assertEquals("non-tabular-resource", round2Package.getResources().get(1).getName());
        Assertions.assertEquals("data-resource", round2Package.getResources().get(1).getProfile());
        Assertions.assertEquals("json", round2Package.getResources().get(1).getFormat());
        Assertions.assertEquals(referenceData, round2Package.getResources().get(1).getRawData());

        // write the package out again
        Path tempDirPathRound3 = Files.createTempDirectory("datapackage-");
        File outFileRound3 = new File (tempDirPathRound3.toFile(), "datapackage.json");
        round2Package.write(outFileRound3, false);

        // read back the datapackage.json and compare to the expected output
        String contentRound3 = String.join("\n", Files.readAllLines(outFileRound3.toPath()));
        Assertions.assertEquals(REFERENCE2.replaceAll("[\n\r]+", "\n"), contentRound3.replaceAll("[\n\r]+", "\n"));

        // read the package back in and check number of resourcces
        Package round3Package = new Package(outFileRound3.toPath(), true);
        Assertions.assertEquals(2, round3Package.getResources().size());

        // compare the non-tabular resource with expected values
        Resource<?> round3Resource = round3Package.getResource("non-tabular-resource");
        Assertions.assertEquals("data-resource", round3Resource.getProfile());
        Object rawData = round3Resource.getRawData();
        Assertions.assertEquals(referenceData, rawData);

    }



    /**
     * A non-tabular resource
     */

    public class myNonTabularResource extends JSONBase implements Resource<File> {

        private Object data;

        private final Path fileName;

        public myNonTabularResource(Object data, Path relativeOutPath) {
            this.data = data;
            super.setName("non-tabular-resource");
            this.fileName = relativeOutPath;
        }

        @Override
        @JsonIgnore
        public List<Table> getTables() {
            return null;
        }

        @Override
        @JsonIgnore
        public Object getRawData() {
            return data;
        }

        @Override
        public List<String[]> getData(boolean b) throws Exception {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public List<Map<String, Object>> getMappedData(boolean b) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public List<Object> getData(boolean b, boolean b1, boolean b2, boolean b3) throws Exception {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @JsonProperty(JSON_KEY_PATH)
        public String getPath() {
            if (File.separator.equals("\\"))
                return fileName.toString().replaceAll("\\\\", "/");
            return fileName.toString();
        }

        @Override
        @JsonIgnore
        public String getDataAsJson() {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        @JsonIgnore
        public String getDataAsCsv() {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public String getDataAsCsv(Dialect dialect, Schema schema) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public List getData(Class aClass) throws Exception {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public void writeData(Path path) throws Exception {
            Path p = path.resolve(fileName);

            try {
                Files.write(p, (byte[])getRawData());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void writeData(Writer writer) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public void writeSchema(Path path) {
        }

        @Override
        public void writeDialect(Path parentFilePath) throws IOException {
        }

        @Override
        public Iterator<Object[]> objectArrayIterator() {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public Iterator<Object[]> objectArrayIterator(boolean b, boolean b1) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public Iterator<Map<String, Object>> mappingIterator(boolean b) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public Iterator beanIterator(Class aClass, boolean b) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        @JsonIgnore
        public Iterator<String[]> stringArrayIterator() {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public Iterator<String[]> stringArrayIterator(boolean b) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        @JsonIgnore
        public String[] getHeaders() {
            return null;
        }

        @Override
        public String getPathForWritingSchema() {
            return null;
        }

        @Override
        @JsonIgnore
        public String getPathForWritingDialect() {
            return null;
        }

        @Override
        @JsonIgnore
        public Set<String> getDatafileNamesForWriting() {
            return null;
        }

        @Override
        public String getName() {
            return super.getName();
        }

        @Override
        public void setName(String s) {
            super.setName(s);
        }

        @Override
        public String getProfile() {
            return PROFILE_DATA_RESOURCE_DEFAULT;
        }

        @Override
        public void setProfile(String s) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public String getTitle() {
            return super.getTitle();
        }

        @Override
        public void setTitle(String s) {
            super.setTitle(s);
        }

        @Override
        public String getDescription() {
            return super.getDescription();
        }

        @Override
        public void setDescription(String s) {
            super.setDescription(s);
        }

        @Override
        public String getMediaType() {
            return super.getMediaType();
        }

        @Override
        public void setMediaType(String s) {
            super.setMediaType(s);
        }

        @Override
        public String getEncoding() {
            return super.getEncoding();
        }

        @Override
        public void setEncoding(String s) {
            super.setEncoding(s);
        }

        @Override
        @JsonIgnore
        public Integer getBytes() {
            return super.getBytes();
        }

        @Override
        public void setBytes(Integer integer) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        @JsonIgnore
        public String getHash() {
            return super.getHash();
        }

        @Override
        public void setHash(String s) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        @JsonIgnore
        public Dialect getDialect() {
            return null;
        }

        @Override
        public void setDialect(Dialect dialect) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public String getFormat() {
            return "pdf";
        }

        @Override
        public void setFormat(String s) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        @JsonIgnore
        public String getDialectReference() {
            return null;
        }

        @Override
        @JsonIgnore
        public Schema getSchema() {
            return null;
        }

        @Override
        public void setSchema(Schema schema) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        public Schema inferSchema() throws TypeInferringException {
            return null;
        }

        @Override
        public List<Source> getSources() {
            return super.getSources();
        }

        @Override
        public void setSources(List<Source> sources) {
            super.setSources(sources);
        }

        @Override
        public List<License> getLicenses() {
            return super.getLicenses();
        }

        @Override
        public void setLicenses(List<License>  licenses) {
            super.setLicenses(licenses);
        }

        @Override
        public boolean shouldSerializeToFile() {
            return true;
        }

        @Override
        public void setShouldSerializeToFile(boolean b) {
        }

        @Override
        public void setSerializationFormat(String s) {
            throw new UnsupportedOperationException("Not supported on non-tabular Resources");
        }

        @Override
        @JsonIgnore
        public String getSerializationFormat() {
            return null;
        }

        @Override
        public void checkRelations(Package aPackage) throws Exception {
        }

        @Override
        public void validate(Package aPackage) {
        }

    }

}
