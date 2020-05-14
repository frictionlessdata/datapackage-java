package io.frictionlessdata.datapackage;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtil {

    public static Path getBasePath() {
        try {
            String pathName = "/fixtures/multi_data_datapackage.json";
            Path sourceFileAbsPath = Paths.get(TestUtil.class.getResource(pathName).toURI());
            return sourceFileAbsPath.getParent();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
