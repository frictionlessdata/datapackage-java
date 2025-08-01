package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;



public class ResourceBuilderTest {
    @Test
    @DisplayName("Test ResourceBuilder with two file paths")
    public void testResourceBuilderWithTwoFiles() throws Exception {
        // Prepare test data
        String[] paths = new String[]{
                "datapackages/multi-data/data/cities.csv",
                "datapackages/multi-data/data/cities2.csv"};
        File basePath = getBasePath();

        // Build resource using ResourceBuilder
        Resource<?> resource = ResourceBuilder.create("city-coordinates")
                .withFiles(basePath, paths)
                .profile(Profile.PROFILE_TABULAR_DATA_RESOURCE)
                .title("City Coordinates")
                .description("Geographic coordinates of various cities")
                .encoding("UTF-8")
                .format(Resource.FORMAT_CSV)
                .build();

        // Verify resource properties
        Assertions.assertEquals("city-coordinates", resource.getName());
        Assertions.assertEquals(Profile.PROFILE_TABULAR_DATA_RESOURCE, resource.getProfile());
        Assertions.assertEquals("City Coordinates", resource.getTitle());
        Assertions.assertEquals("Geographic coordinates of various cities", resource.getDescription());
        Assertions.assertEquals("UTF-8", resource.getEncoding());
        Assertions.assertEquals(Resource.FORMAT_CSV, resource.getFormat());

        // Verify the resource is file-based and has correct paths
        Assertions.assertInstanceOf(FilebasedResource.class, resource);
        FilebasedResource fileResource = (FilebasedResource) resource;

        // Test that we can iterate through data from both files
        Iterator<Object[]> iter = resource.objectArrayIterator();
        int recordCount = 0;
        while (iter.hasNext()) {
            Object[] record = iter.next();
            Assertions.assertEquals(2, record.length); // city and location columns
            recordCount++;
        }

        // Should have 6 records total (3 from each file)
        Assertions.assertEquals(6, recordCount);
    }

    @Test
    @DisplayName("Test ResourceBuilder with multiple files and schema inference")
    public void testResourceBuilderWithMultipleFilesAndSchemaInference() throws Exception {
        // Prepare test files
        String[] paths = new String[]{"data/cities.csv", "data/cities2.csv"};
        File basePath = getBasePath();
        List<File> files = new ArrayList<>();
        for (String path : paths) {
            files.add(new File(path));
        }

        // Build resource using ResourceBuilder with schema inference
        Resource<?> resource = ResourceBuilder.create("multi-cities")
                .withFiles(getBasePath(), files)
                .inferSchema()
                .build();

        // Verify resource was created correctly
        Assertions.assertNotNull(resource);
        Assertions.assertEquals("multi-cities", resource.getName());
        Assertions.assertInstanceOf(FilebasedResource.class, resource);

        // Verify schema was inferred
        Schema inferredSchema = resource.getSchema();
        Assertions.assertNotNull(inferredSchema, "Schema should be inferred");
        Assertions.assertEquals(2, inferredSchema.getFields().size(), "Should have 2 fields");

        // Verify field names from inferred schema
        Assertions.assertEquals("city", inferredSchema.getFields().get(0).getName());
        Assertions.assertEquals("location", inferredSchema.getFields().get(1).getName());

        // Verify data can be read correctly with the inferred schema
        List<Object> data = resource.getData(false, false, false, false);
        Assertions.assertEquals(6, data.size(), "Should have 6 rows total from both files");

        // Verify first row from first file
        Object[] firstRow = (Object[])data.get(0);
        Assertions.assertEquals("libreville", firstRow[0]);
        Assertions.assertEquals("0.41,9.29", firstRow[1]);

        // Verify first row from second file (4th row overall)
        Object[] fourthRow =(Object[]) data.get(3);
        Assertions.assertEquals("barranquilla", fourthRow[0]);
        Assertions.assertEquals("10.98,-74.88", fourthRow[1]);
    }

    @Test
    @DisplayName("Create non-tabular resource with PDF file using ResourceBuilder")
    public void testCreateNonTabularResourceWithPdfFile() throws Exception {
        String fileName = "sample.pdf";
        // Get the PDF file path
        File pdf = new File(fileName);
        File basePath = new File(getBasePath(), "files");

        // Build a non-tabular resource with PDF
        Resource<?> resource = ResourceBuilder.create("sample-pdf")
                .withFile(basePath, pdf)
                .format(null)
                .profile(Profile.PROFILE_DATA_RESOURCE_DEFAULT)
                .title("Sample PDF Document")
                .description("A sample PDF file for testing non-tabular resources")
                .mediaType("application/pdf")
                .build();

        // Verify the resource properties
        Assertions.assertNotNull(resource);
        Assertions.assertEquals("sample-pdf", resource.getName());
        Assertions.assertEquals("pdf", resource.getFormat());
        Assertions.assertEquals(Profile.PROFILE_DATA_RESOURCE_DEFAULT, resource.getProfile());
        Assertions.assertEquals("Sample PDF Document", resource.getTitle());
        Assertions.assertEquals("A sample PDF file for testing non-tabular resources", resource.getDescription());
        Assertions.assertEquals("application/pdf", resource.getMediaType());

        // Verify it's a file-based resource
        Assertions.assertInstanceOf(FilebasedResource.class, resource);

        File tempDirPath = Files.createTempDirectory("datapackage-").toFile();
        File outFile = new File(tempDirPath, fileName);

        // Write the resource to a file
        resource.writeData(tempDirPath.toPath());
        Assertions.assertTrue(outFile.exists());
        Assertions.assertTrue(outFile.length() > 0, "Output file should not be empty");
        // read the file back to verify content
        String content = new String(Files.readAllBytes(outFile.toPath()), StandardCharsets.UTF_8);
        // compare the content with the original PDF file
        String originalContent = new String(Files.readAllBytes(new File(basePath, fileName).toPath()), StandardCharsets.UTF_8);
        Assertions.assertEquals(originalContent, content, "Content of the written PDF should match the original");

    }

    private static File getBasePath() throws URISyntaxException {
        URL sourceFileUrl = ResourceBuilderTest.class.getResource("/fixtures/data");
        Path path = Paths.get(sourceFileUrl.toURI());
        return path.getParent().toFile();
    }
}
