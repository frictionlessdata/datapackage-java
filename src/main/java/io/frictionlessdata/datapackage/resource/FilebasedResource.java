package io.frictionlessdata.datapackage.resource;

import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.Table;
import org.apache.commons.csv.CSVFormat;
import io.frictionlessdata.tableschema.datasourceformat.DataSourceFormat;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;


@JsonInclude(value = Include.NON_EMPTY, content = Include.NON_EMPTY )
public class FilebasedResource<C> extends AbstractReferencebasedResource<File,C> {
    private File basePath;
    private boolean isInArchive;

    public FilebasedResource(Resource fromResource, Collection<File> paths) throws Exception {
        super(fromResource.getName(), paths);
        if (null == paths) {
            throw new DataPackageException("Invalid Resource. " +
                    "The path property cannot be null for file-based Resources.");
        }
        this.setSerializationFormat(sniffFormat(paths));
        schema = fromResource.getSchema();
        dialect = fromResource.getDialect();
        List<String[]> data = fromResource.getData(false, false, false, false);
        Table table = new Table(data, fromResource.getHeaders(), fromResource.getSchema());
        tables = new ArrayList<>();
        tables.add(table);
        serializeToFile = true;
    }

    FilebasedResource(String name, Collection<File> paths, File basePath) {
        super(name, paths);
        if (null == paths) {
            throw new DataPackageException("Invalid Resource. " +
                    "The path property cannot be null for file-based Resources.");
        }
        this.setSerializationFormat(sniffFormat(paths));
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
        serializeToFile = true;
    }

    private static String sniffFormat(Collection<File> paths) {
        Set<String> foundFormats = new HashSet<>();
        paths.forEach((p) -> {
            if (p.getName().toLowerCase().endsWith(DataSourceFormat.Format.FORMAT_CSV.getLabel())) {
                foundFormats.add(DataSourceFormat.Format.FORMAT_CSV.getLabel());
            } else if (p.getName().toLowerCase().endsWith(DataSourceFormat.Format.FORMAT_JSON.getLabel())) {
                foundFormats.add(DataSourceFormat.Format.FORMAT_JSON.getLabel());
            }
        });
        if (foundFormats.size() > 1) {
            throw new DataPackageException("Resources cannot be mixed JSON/CSV");
        }
        if (foundFormats.isEmpty())
            return DataSourceFormat.Format.FORMAT_CSV.getLabel();
        return foundFormats.iterator().next();
    }

    public static FilebasedResource fromSource(String name, Collection<File> paths, File basePath) {
        return new FilebasedResource(name, paths, basePath);
    }

    @JsonIgnore
    public File getBasePath() {
        return basePath;
    }

    @Override
    Table createTable(File reference) throws Exception {
        return Table.fromSource(reference, basePath, schema, getCsvFormat());
    }

    @Override
    String getStringRepresentation(File reference) {
        if (File.separator.equals("\\"))
            return reference.getPath().replaceAll("\\\\", "/");
        return reference.getPath();
    }

    @Override
    List<Table> readData () throws Exception{
        List<Table> tables;
        if (this.isInArchive) {
            tables = readfromZipFile();
        } else {
            tables = readfromOrdinaryFile();
        }
        return tables;
    }

    private List<Table> readfromZipFile() throws Exception {
        List<Table> tables = new ArrayList<>();
        for (File file : paths) {
            String fileName = file.getPath().replaceAll("\\\\", "/");
            String content = getZipFileContentAsString (basePath.toPath(), fileName);
            Table table = Table.fromSource(content, schema, getCsvFormat());
            tables.add(table);
        }
        return tables;
    }
    private List<Table> readfromOrdinaryFile() throws Exception {
        List<Table> tables = new ArrayList<>();
        for (File file : paths) {
                /* from the spec: "SECURITY: / (absolute path) and ../ (relative parent path)
                   are forbidden to avoid security vulnerabilities when implementing data
                   package software."

                   https://frictionlessdata.io/specs/data-resource/index.html#url-or-path
                 */
            Path securePath = Resource.toSecure(file.toPath(), basePath.toPath());
            Path relativePath = basePath.toPath().relativize(securePath);
            Table table = createTable(relativePath.toFile());
            tables.add(table);
        }
        return tables;
    }
/*
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
    */
    public void setIsInArchive(boolean isInArchive) {
        this.isInArchive = isInArchive;
    }
}
