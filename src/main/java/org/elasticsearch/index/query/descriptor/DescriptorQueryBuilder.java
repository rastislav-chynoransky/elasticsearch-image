package org.elasticsearch.index.query.descriptor;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.descriptor.DescriptorMapper;
import org.elasticsearch.index.query.BoostableQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

public class DescriptorQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<DescriptorQueryBuilder> {

    private final String fieldName;

    private float boost = -1;

    private String hash;

    private double[] descriptor;

    private String lookupIndex;

    private String lookupType;

    private String lookupField;

    private String lookupId;

    private String lookupRouting;

    public DescriptorQueryBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public DescriptorQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public DescriptorQueryBuilder hash(String hash) {
        this.hash = hash;
        return this;
    }

    public DescriptorQueryBuilder descriptor(double[] descriptor) {
        this.descriptor = descriptor;
        return this;
    }

    public DescriptorQueryBuilder lookupIndex(String lookupIndex) {
        this.lookupIndex = lookupIndex;
        return this;
    }

    public DescriptorQueryBuilder lookupField(String lookupField) {
        this.lookupField = lookupField;
        return this;
    }

    public DescriptorQueryBuilder lookupType(String lookupType) {
        this.lookupType = lookupType;
        return this;
    }

    public DescriptorQueryBuilder lookupId(String lookupId) {
        this.lookupId = lookupId;
        return this;
    }

    public DescriptorQueryBuilder lookupRouting(String lookupRouting) {
        this.lookupRouting = lookupRouting;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject(DescriptorQueryParser.NAME); // todo ?

        builder.startObject(fieldName);
        builder.field("hash", hash);
        builder.field("descriptor", descriptor);

        if (boost != -1) {
            builder.field("boost", boost);
        }

        if (lookupIndex != null) {
            builder.field("index", lookupIndex);
        }

        if (lookupType != null) {
            builder.field("type", lookupType);
        }

        if (lookupField != null) {
            builder.field("field", lookupField);
        }

        if (lookupId != null) {
            builder.field("id", lookupId);
        }

        if (lookupRouting != null) {
            builder.field("routing", lookupRouting);
        }

        builder.endObject();
        builder.endObject();
    }
}
