package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.exception.ForeignKeyException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ForeignKeysTest {

    @Test
    @DisplayName("Test that foreign keys are validated correctly, good case")
    void testForeignKeysGoodCase() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign_keys_valid.json");
        Package pkg = new Package(resourcePath, true);
        pkg.getResource("teams");
    }

    @Test
    @DisplayName("Test that foreign keys are validated correctly, bad case")
    void testForeignKeysBadCase() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign_keys_invalid.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");

        DataPackageValidationException ex = assertThrows(DataPackageValidationException.class,
                () -> teams.checkRelations(pkg));
        Throwable cause = ex.getCause();
        Assertions.assertInstanceOf(ForeignKeyException.class, cause);
        Assertions.assertEquals("Foreign key validation failed: [city] -> [name]: 'Munich' not found in resource 'cities'.", cause.getMessage());
    }
}
