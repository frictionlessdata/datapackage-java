package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.datasources.DataSource;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.frictionlessdata.datapackage.datareference.DataReference.getFileContentAsString;

public class FilebasedResource extends AbstractReferencebasedResource<File> {
    private File basePath;

    public FilebasedResource(String name, Collection<File> paths, File basePath) {
        super(name, paths);
        if (null == paths) {
            throw new DataPackageException("Invalid Resource. " +
                    "The path property cannot be null for file-based Resources.");
        }
        this.basePath = basePath;
        for (File path : paths) {
            /* from the spec: "SECURITY: / (absolute path) and ../ (relative parent path)
               are forbidden to avoid security vulnerabilities when implementing data
               package software."

               https://frictionlessdata.io/specs/data-resource/index.html#url-or-path
             */
            if (path.isAbsolute()) {
                throw new DataPackageException("Path entries for file-based Resources cannot be absolute");
            }
        }
    }

    public File getBasePath() {
        return basePath;
    }

    @Override
    Table createTable(File reference) throws Exception {
        return  (schema != null)
                ? new Table(reference, basePath, schema)
                : new Table(reference, basePath);
    }

    @Override
    String getStringRepresentation(File reference) {
        if (File.separator.equals("\\"))
            return reference.getPath().replaceAll("\\\\", "/");
        return reference.getPath();
    }


    @Override
    List<Table> readData () throws Exception{
        List<Table> tables = new ArrayList<>();
        // If the path of a data file has been set.
        if (super.paths != null){
            for (File file : paths) {
                /* from the spec: "SECURITY: / (absolute path) and ../ (relative parent path)
                   are forbidden to avoid security vulnerabilities when implementing data
                   package software."

                   https://frictionlessdata.io/specs/data-resource/index.html#url-or-path
                 */
                Path securePath = DataSource.toSecure(file.toPath(), basePath.toPath());
                Path relativePath = basePath.toPath().relativize(securePath);
                Table table = createTable(relativePath.toFile());
                setCsvFormat(table);
                tables.add(table);
            }
        }
        return tables;
    }

    @Override
    public void writeDataAsCsv(Path outputDir, Dialect dialect) throws Exception {
        Dialect lDialect = (null != dialect) ? dialect : Dialect.DEFAULT;
        List<String> paths = new ArrayList<>(getReferencesAsStrings());
        int cnt = 0;
        for (String fileName : paths) {
            List<Table> tables = getTables();
            Table t  = tables.get(cnt++);
            Path p;
            if (outputDir.toString().isEmpty()) {
                p = outputDir.getFileSystem().getPath(fileName);
                if (!Files.exists(p)) {
                    Files.createDirectories(p);
                }
            } else {
                if (!Files.exists(outputDir)) {
                    Files.createDirectories(outputDir);
                }
                p = outputDir.resolve(fileName);
            }

            Files.deleteIfExists(p);
            writeTableAsCsv(t, lDialect, p);
        }
    }
}
