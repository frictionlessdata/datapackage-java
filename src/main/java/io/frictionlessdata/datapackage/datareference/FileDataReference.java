package io.frictionlessdata.datapackage.datareference;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.Collection;
import java.util.LinkedHashSet;

public class FileDataReference extends AbstractDataReference<File> {

    private File basePath;

    FileDataReference(Collection<File> source, File basePath) {
        super(source);
        this.basePath = basePath;
    }

    @Override
    public Collection<String> getReferencesAsStrings() {
        Collection<String> retVal = new LinkedHashSet<>();
        Collection<File> elements = resolve();
        for (File f : elements) {
            String pathName = f.getPath();
            if (File.separator.equals("\\"))
                pathName = pathName.replaceAll("\\\\", "/");
            retVal.add(pathName);
        }
        return retVal;
    }

    public File getBasePath() {
        return basePath;
    }
}
