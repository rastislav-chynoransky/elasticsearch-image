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

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject(DescriptorQueryParser.NAME);

        builder.startObject(fieldName);
        builder.field(DescriptorMapper.HASH, hash);
        builder.field(DescriptorMapper.CONTENT_TYPE, descriptor);

        if (boost != -1) {
            builder.field("boost", boost);
        }

        builder.endObject();
        builder.endObject();
    }
}
