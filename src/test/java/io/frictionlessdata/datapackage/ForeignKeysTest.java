package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.resource.Resource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

public class ForeignKeysTest {

    @Test
    @DisplayName("Test that foreign keys are validated correctly")
    void testValidationURLAsSchemaReference() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign-keys.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");
        teams.checkRelations(pkg);
        List data = teams.getData(true);
        System.out.println("Data: " + data);
    }
}
