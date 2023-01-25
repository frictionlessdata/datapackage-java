package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.exception.ForeignKeyException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

public class ForeignKeysTest {

    @Test
    @DisplayName("Test that a schema can define foreign keys and our code resolves them")
        // Test for https://github.com/frictionlessdata/datapackage-java/issues/4
    void testForeignKeyValidation() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign-keys.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");
        teams.checkRelations(pkg.getResources());
    }

    @Test
    @DisplayName("Test that a schema can define foreign keys and our code resolves them -> must throw on invalid ref")
        // Test for https://github.com/frictionlessdata/datapackage-java/issues/4
    void testForeignKeyValidationError() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign-keys-invalid.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");
        ForeignKeyException fke = Assertions.assertThrows(
                ForeignKeyException.class, () -> teams.checkRelations(pkg.getResources()));
        Assertions.assertEquals("Foreign key 'city' violation.", fke.getMessage());
    }

    @Test
    @DisplayName("Test that a schema can define compound foreign keys and our code resolves them")
        // Test for https://github.com/frictionlessdata/datapackage-java/issues/4
    void testCompoundForeignKeyValidation() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign-keys-extended.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");
        teams.checkRelations(pkg.getResources());
    }

    @Test
    @DisplayName("Test that a schema with mismatched String and Array foreign key must throw")
    void testCompoundForeignKeyValidationError() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign-keys-extended-invalid1.json");
        ForeignKeyException fke = Assertions.assertThrows(
                ForeignKeyException.class, () -> new Package(resourcePath, true));
        Assertions.assertEquals(
                "The reference's fields property must be an array if the outer fields is an array.",
                fke.getMessage());
    }

    @Test
    @DisplayName("Test that a schema with mismatched String and Array foreign key must throw")
    void testCompoundForeignKeyValidationError2() throws Exception{
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/foreign-keys-extended-invalid2.json");
        ForeignKeyException fke = Assertions.assertThrows(
                ForeignKeyException.class, () -> new Package(resourcePath, true));
        Assertions.assertEquals(
                "The reference's fields property must be a string if the outer fields is a string.",
                fke.getMessage());
    }
}
