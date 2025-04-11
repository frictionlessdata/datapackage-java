package io.frictionlessdata.datapackage.fk;

import io.frictionlessdata.datapackage.Package;
import io.frictionlessdata.datapackage.resource.Resource;
import io.frictionlessdata.tableschema.Table;
import io.frictionlessdata.tableschema.exception.ForeignKeyException;
import io.frictionlessdata.tableschema.field.Field;
import io.frictionlessdata.tableschema.fk.ForeignKey;
import io.frictionlessdata.tableschema.fk.Reference;
import io.frictionlessdata.tableschema.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * PackageForeignKey is a wrapper around the {@link io.frictionlessdata.tableschema.fk.ForeignKey} class to validate foreign keys
 * in the context of a data package. It checks if the referenced resource and fields exist
 * in the data package and validates the foreign key constraints.
 *
 * This class exists because the specification of foreign keys in the tableschema specification
 * is a bit awkward: it assumes that the target of a foreign key can be resolved to a different table, which is
 * only possible on a datapackage level, yet the foreign key is defined as part of a Schema.
 *
 * In our implementation therefore, we have a ForeignKey class in the TableSchema package which only can
 * validate self-referencing FKs. This class is used to resolve FKs to different resources of a package
 */
public class PackageForeignKey {

    private ForeignKey fk;
    private Package datapackage;
    private Resource<?> resource;

    public PackageForeignKey(ForeignKey fk, Resource<?> res, Package pkg) {
        this.datapackage = pkg;
        this.resource = res;
        this.fk = fk;
    }

    /**
     * Formal validation of the foreign key. This method checks if the referenced resource and fields exist.
     * It does not check the actual data in the tables.
     *
     * Verification of table data against the foreign key constraints is done in
     * {@link io.frictionlessdata.datapackage.resource.AbstractResource#checkRelations}.
     *
     * @throws Exception if the foreign key relation is invalid.
     */
    public void validate() throws Exception {
        Reference reference = fk.getReference();
        // self-reference, this can be validated by the Tableschema {@link io.frictionlessdata.tableschema.fk.ForeignKey} class
        if (reference.getResource().equals("")) {
           for (Table table : resource.getTables()) {
                fk.validate(table);
            }
        } else {
            // validate the foreign key
            Resource<?> refResource = datapackage.getResource(reference.getResource());
            if (refResource == null) {
                throw new ForeignKeyException("Reference resource not found: " + reference.getResource());
            }
            List<String> fieldNames = new ArrayList<>();
            List<String> foreignFieldNames = new ArrayList<>();
            List<String> lFields = fk.getFieldNames();
            Schema foreignSchema = refResource.getSchema();
            if (null == foreignSchema) {
                foreignSchema = refResource.inferSchema();
            }
            for (int i = 0; i < lFields.size(); i++) {
                fieldNames.add(lFields.get(i));
                String foreignFieldName = reference.getFieldNames().get(i);
                foreignFieldNames.add(foreignFieldName);
                Field<?> foreignField = foreignSchema.getField(foreignFieldName);
                if (null == foreignField) {
                    throw new ForeignKeyException("Foreign key ["+fieldNames.get(i)+ "-> "
                            +reference.getFieldNames().get(i)+"] violation : expected: "
                            +reference.getFieldNames().get(i) + ", but not found");
                }
            }

        }
    }

    public ForeignKey getForeignKey() {
        return fk;
    }

    public Package getDatapackage() {
        return datapackage;
    }

    public Resource<?> getResource() {
        return resource;
    }
}
