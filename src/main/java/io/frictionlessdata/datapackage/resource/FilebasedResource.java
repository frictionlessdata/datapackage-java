package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.datapackage.exceptions.DataPackageValidationException;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
        encoding = fromResource.getEncoding();
        this.setSerializationFormat(sniffFormat(paths));
        schema = fromResource.getSchema();
        dialect = fromResource.getDialect();
        List<String[]> data = fromResource.getData(false, false, false, false);
        Table table = new Table(data, fromResource.getHeaders(), fromResource.getSchema());
        tables = new ArrayList<>();
        tables.add(table);
        serializeToFile = true;
    }

    public FilebasedResource(String name, Collection<File> paths, File basePath, Charset encoding) {
        super(name, paths);
        this.encoding = encoding.name();
        if (null == paths) {
            throw new DataPackageValidationException("Invalid Resource. " +
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
                throw new DataPackageValidationException("Path entries for file-based Resources cannot be absolute");
            }
        }
        serializeToFile = true;
    }

    public FilebasedResource(String name, Collection<File> paths, File basePath) {
        this(name, paths, basePath, Charset.defaultCharset());
    }

    private static String sniffFormat(Collection<File> paths) {
        Set<String> foundFormats = new HashSet<>();
        paths.forEach((p) -> {
            if (p.getName().toLowerCase().endsWith(TableDataSource.Format.FORMAT_CSV.getLabel())) {
                foundFormats.add(TableDataSource.Format.FORMAT_CSV.getLabel());
            } else if (p.getName().toLowerCase().endsWith(TableDataSource.Format.FORMAT_JSON.getLabel())) {
                foundFormats.add(TableDataSource.Format.FORMAT_JSON.getLabel());
            }
        });
        if (foundFormats.size() > 1) {
            throw new DataPackageException("Resources cannot be mixed JSON/CSV");
        }
        if (foundFormats.isEmpty())
            return TableDataSource.Format.FORMAT_CSV.getLabel();
        return foundFormats.iterator().next();
    }

    public static FilebasedResource fromSource(String name, Collection<File> paths, File basePath, Charset encoding) {
        FilebasedResource r = new FilebasedResource(name, paths, basePath);
        r.encoding = encoding.name();
        return r;
    }

    @JsonIgnore
    public File getBasePath() {
        return basePath;
    }

    @Override
    Table createTable(File reference, Charset encoding) {
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
        Charset cs = getEncodingOrDefault();
        for (File file : paths) {
            String fileName = file.getPath().replaceAll("\\\\", "/");
            String content = getZipFileContentAsString (basePath.toPath(), fileName, cs);
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
            Charset cs = getEncodingOrDefault();

            Table table = createTable(relativePath.toFile(), cs);
            tables.add(table);
        }
        return tables;
    }

    public void setIsInArchive(boolean isInArchive) {
        this.isInArchive = isInArchive;
    }
}
