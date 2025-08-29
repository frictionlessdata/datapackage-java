package io.frictionlessdata.datapackage.fk;

import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.TestUtil;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.exception.ForeignKeyException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ForeignKeyTest {

    @Test
    @DisplayName("Test that foreign keys are validated correctly, good case")
    void testForeignKeysGoodCase() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign_keys_valid.json");
        io.frictionlessdata.datapackage.Package pkg = new io.frictionlessdata.datapackage.Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");
        teams.checkRelations(pkg);
    }

    @Test
    @DisplayName("Test that foreign keys are validated correctly, bad case")
    void testForeignKeysBadCase() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign_keys_invalid.json");
        io.frictionlessdata.datapackage.Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");

        DataPackageValidationException ex = assertThrows(DataPackageValidationException.class,
                () -> teams.checkRelations(pkg));
        Throwable cause = ex.getCause();
        Assertions.assertInstanceOf(ForeignKeyException.class, cause);
        Assertions.assertEquals("Foreign key validation failed: [city] -> [name]: 'Munich' not found in resource 'cities'.", cause.getMessage());
    }

    @Test
    @DisplayName("Test checkRelations on valid resources in different-valid-data-formats datapackage")
    void testCheckRelationsOnAllResources() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/different-valid-data-formats/datapackage.json");
        Package dp = new Package(resourcePath, true);

        dp.getResource("teams_with_headers_csv_file").checkRelations(dp);
        dp.getResource("teams_arrays_inline").checkRelations(dp);
        dp.getResource("teams_objects_inline").checkRelations(dp);
        dp.getResource("teams_arrays_file").checkRelations(dp);
        dp.getResource("teams_objects_file").checkRelations(dp);
    }
}
