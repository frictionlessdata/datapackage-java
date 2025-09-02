/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.frictionlessdata.datapackage;

import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.exception.PrimaryKeyException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrimaryKeysTest {

    @Test
    @DisplayName("Test the uniqueness of simple primary keys - invalid case")
    void testPrimaryKeysUniqueInvalid() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/primary-keys/simple/primary_keys_csv_invalid.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");

        Throwable ex = assertThrows(Exception.class, teams::checkPrimaryKeys);
        assertInstanceOf(PrimaryKeyException.class, ex);
        assertEquals("Error validating primary keys: Primary key violation in resource 'teams': duplicate key 1", ex.getMessage());
    }

    @Test
    @DisplayName("Test the uniqueness of simple primary keys - valid case")
    void testPrimaryKeysUniqueValid() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/primary-keys/simple/primary_keys_csv_valid.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");

        assertDoesNotThrow(teams::checkPrimaryKeys);
    }

    @Test
    @DisplayName("Test the uniqueness of composite primary keys - invalid case")
    void testCompositePrimaryKeysUniqueInvalid() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/primary-keys/composite/primary_keys_csv_invalid.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");

        Throwable ex = assertThrows(Exception.class, teams::checkPrimaryKeys);
        assertInstanceOf(PrimaryKeyException.class, ex);
        assertEquals("Error validating primary keys: Primary key violation in resource 'teams': duplicate key UK\tLondon", ex.getMessage());
    }

    @Test
    @DisplayName("Test the uniqueness of composite primary keys - valid case")
    void testCompositePrimaryKeysUniqueValid() throws Exception {
        Path resourcePath = TestUtil.getResourcePath("/fixtures/datapackages/primary-keys/composite/primary_keys_csv_valid.json");
        Package pkg = new Package(resourcePath, true);
        Resource teams = pkg.getResource("teams");

        assertDoesNotThrow(teams::checkPrimaryKeys);
    }
}
