package io.frictionlessdata.datapackage.resource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.frictionlessdata.datapackage.Dialect;
import io.frictionlessdata.datapackage.Profile;
import io.frictionlessdata.datapackage.exceptions.DataPackageException;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.schema.Schema;
import io.frictionlessdata.tableschema.tabledatasource.TableDataSource;
import io.frictionlessdata.tableschema.util.JsonUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder for creating different types of Resources with a fluent API
 */
public class ResourceBuilder {
    private String name;
    private String format;
    private String profile;
    private Schema schema;
    private Dialect dialect;
    private String title;
    private String description;
    private String encoding;
    private String mediaType;
    private boolean serializeToFile = false;
    private String serializationFormat;

    private boolean shouldInferSchema = false;

    // Data source fields
    private Object data;
    private List<File> paths;
    private List<URL> urls;
    private File basePath;

    private ResourceBuilder(String name) {
        this.name = name;
    }

    /**
     * Start building a new Resource with the given name
     */
    public static ResourceBuilder create(String name) {
        return new ResourceBuilder(name);
    }

    /**
     * Sniff string data for the data type
     */
    public ResourceBuilder withData(String data) {
        try {
            JsonNode json = JsonUtil.getInstance().readValue(data);
            this.format = Resource.FORMAT_JSON;
            if (json instanceof ArrayNode) {
                withJsonArrayData((ArrayNode)json);
            } else  {
                withJsonObjectData((ObjectNode) json);
            }
        } catch (Exception ex) {
            // might be CSV or other format, check later
            this.data = data;
        }

        return this;
    }

    /**
     * Set CSV string data for the resource
     */
    public ResourceBuilder withCsvData(String csvData) {
        this.data = csvData;
        this.format = Resource.FORMAT_CSV;
        return this;
    }

    /**
     * Set JSON array data for the resource
     */
    public ResourceBuilder withJsonArrayData(ArrayNode jsonArray) {
        this.data = jsonArray;
        this.format = Resource.FORMAT_JSON;
        return this;
    }

    /**
     * Set JSON object data for non-tabular resource
     */
    public ResourceBuilder withJsonObjectData(ObjectNode jsonObject) {
        this.data = jsonObject;
        this.format = Resource.FORMAT_JSON;
        this.profile = Profile.PROFILE_DATA_RESOURCE_DEFAULT;
        return this;
    }

    /**
     * Set files for the resource
     */
    public ResourceBuilder withFiles(File basePath, Collection<File> paths) {
        this.paths = new ArrayList<>(paths);
        this.basePath = basePath;
        return this;
    }

    /**
     * Set file paths for the resource
     */
    public ResourceBuilder withFiles(File basePath, String... paths) {
        this.paths = Arrays.stream(paths).map(File::new).collect(Collectors.toList());
        this.basePath = basePath;
        return this;
    }

    /**
     * Set a single file path for the resource
     */
    public ResourceBuilder withFile(File basePath, String path) {
        if (null == this.paths) {
            this.paths = new ArrayList<>();
        }
        this.paths.add(new File(path));
        this.basePath = basePath;
        return this;
    }

    /**
     * Set a single file for the resource
     */
    public ResourceBuilder withFile(File basePath, File path) {
        if (null == this.paths) {
            this.paths = new ArrayList<>();
        }
        this.paths.add(path);
        this.basePath = basePath;
        return this;
    }

    /**
     * Set URLs for the resource
     */
    public ResourceBuilder withUrls(List<URL> urls) {
        this.urls = new ArrayList<>(urls);
        return this;
    }

    /**
     * Set a single URL for the resource
     */
    public ResourceBuilder withUrl(URL url) {
        this.urls = new ArrayList<>();
        this.urls.add(url);
        return this;
    }

    /**
     * Set custom format (overrides auto-detection)
     */
    public ResourceBuilder format(String format) {
        this.format = format;
        return this;
    }

    /**
     * Set the profile (default: tabular-data-resource for tabular data)
     */
    public ResourceBuilder profile(String profile) {
        this.profile = profile;
        return this;
    }

    /**
     * Set the schema
     */
    public ResourceBuilder schema(Schema schema) {
        this.schema = schema;
        return this;
    }

    /**
     * Infer the schema from the data source.
     */
    public ResourceBuilder inferSchema() {
        this.shouldInferSchema = true;
        return this;
    }

    /**
     * Set the dialect
     */
    public ResourceBuilder dialect(Dialect dialect) {
        this.dialect = dialect;
        return this;
    }

    /**
     * Set whether to serialize inline data to file when saving package
     */
    public ResourceBuilder serializeToFile(boolean serialize) {
        this.serializeToFile = serialize;
        return this;
    }

    /**
     * Set the format for serialization (csv or json)
     */
    public ResourceBuilder serializationFormat(String format) {
        this.serializationFormat = format;
        return this;
    }

    /**
     * Set the title
     */
    public ResourceBuilder title(String title) {
        this.title = title;
        return this;
    }

    /**
     * Set the description
     */
    public ResourceBuilder description(String description) {
        this.description = description;
        return this;
    }

    /**
     * Set the encoding
     */
    public ResourceBuilder encoding(String encoding) {
        this.encoding = encoding;
        return this;
    }

    /**
     * Set the media type
     */
    public ResourceBuilder mediaType(String mediaType) {
        this.mediaType = mediaType;
        return this;
    }



    /**
     * Build the Resource instance
     */
    public Resource<?> build() {
        Resource<?> resource = null;

        if (null == encoding) {
            encoding = TableDataSource.getDefaultEncoding().toString();
        }
        Charset charset = Charset.forName(encoding);

        if (shouldInferSchema) {
            if (null != schema) {
                throw new IllegalStateException("Cannot infer schema when schema is already provided");
            }
            if (null != data) {
                schema = Schema.infer(data, charset);
            } else if (null != paths) {
                List<File> files = paths.stream().map((f) -> new File(basePath, f.getPath())).collect(Collectors.toList());
                schema = Schema.infer(files, charset);
            } else if (null != urls) {
                schema = Schema.infer(urls, charset);
            }
        }

        // Create appropriate resource type based on provided data
        if (data != null) {
            if (null == profile) {
                profile = Profile.PROFILE_TABULAR_DATA_RESOURCE;
            }
            if (format != null && format.equals(Resource.FORMAT_CSV)) {
                resource = new CSVDataResource(name, (String) data);
            } else if (format != null && format.equals(Resource.FORMAT_JSON)) {
                if (data instanceof String) {
                    resource = new JSONDataResource(name, (String) data);
                } else if (data instanceof ObjectNode) {
                    resource = new JSONObjectResource(name, (ObjectNode)data);
                } else if (data instanceof ArrayNode) {
                    resource = new JSONDataResource(name, (ArrayNode)data);
                }
            } else {
                try {
                    CSVParser.parse((String)data, TableDataSource.getDefaultCsvFormat()).getHeaderMap();
                    this.format = Resource.FORMAT_CSV;
                } catch (Exception ex2) {
                    throw new IllegalStateException("Cannot determine resource type from data, neigher JSON nor CSV format detected");
                }
                throw new IllegalStateException("Cannot determine resource type from data");
            }
        } else if (paths != null && !paths.isEmpty()) {
            try {
                resource = Resource.build(name, paths, basePath, charset);
            } catch (Exception e) {
                throw new DataPackageException(e);
            }
        } else if (urls != null && !urls.isEmpty()) {
            try {
                resource = Resource.build(name, urls, null, charset);
            } catch (Exception e) {
                throw new DataPackageException(e);
            }
        } else {
            throw new IllegalStateException("No data source provided for resource");
        }

        // Set common properties
        if (profile != null) {
            resource.setProfile(profile);
        }
        if (schema != null) {
            resource.setSchema(schema);
        }
        if (dialect != null) {
            resource.setDialect(dialect);
        }
        if (title != null) {
            resource.setTitle(title);
        }
        if (description != null) {
            resource.setDescription(description);
        }
        if (encoding != null) {
            resource.setEncoding(encoding);
        }
        if (mediaType != null) {
            resource.setMediaType(mediaType);
        }
        if (format != null && resource.getFormat() == null) {
            resource.setFormat(format);
        }

        resource.setShouldSerializeToFile(serializeToFile);
        if (serializationFormat != null) {
            resource.setSerializationFormat(serializationFormat);
        }

        return resource;
    }

}