package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.resource.Resource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

public class ForeignKeysTest {

    @Test
    @DisplayName("Test that a schema can be defined via a URL")
        // Test for https://github.com/frictionlessdata/specs/issues/645
    void testValidationURLAsSchemaReference() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign-keys.json");
        Package pkg = new Package(resourcePath, true);
        System.out.println(pkg);
        Resource teams = pkg.getResource("teams");
        teams.checkRelations();
    }
}
