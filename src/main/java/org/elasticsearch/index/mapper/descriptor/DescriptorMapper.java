package org.elasticsearch.index.mapper.descriptor;

import net.semanticmetadata.lire.indexers.hashing.BitSampling;
import net.semanticmetadata.lire.indexers.hashing.LocalitySensitiveHashing;
import net.semanticmetadata.lire.utils.SerializationUtils;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.image.HashEnum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.stringField;

public class DescriptorMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "descriptor";
    private static ESLogger logger = ESLoggerFactory.getLogger(DescriptorMapper.class.getName());

    public static final String HASH = "hash";

    public static final String BIT_SAMPLING_FILE = "/hash/LshBitSampling.obj";
    public static final String LSH_HASH_FILE = "/hash/lshHashFunctions.obj";

    static {
        try {
            BitSampling.readHashFunctions(DescriptorMapper.class.getResourceAsStream(BIT_SAMPLING_FILE));
            LocalitySensitiveHashing.readHashFunctions(DescriptorMapper.class.getResourceAsStream(LSH_HASH_FILE));
        } catch (IOException e) {
            logger.error("Failed to initialize hash function", e);
        }
    }

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new DescriptorFieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }
    }

    static final class DescriptorFieldType extends MappedFieldType {

        public DescriptorFieldType() {}

        protected DescriptorFieldType(DescriptorMapper.DescriptorFieldType ref) {
            super(ref);
        }

        @Override
        public DescriptorMapper.DescriptorFieldType clone() {
            return new DescriptorMapper.DescriptorFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public String value(Object value) {
            return value == null ? null : value.toString();
        }
    }

    public static class Builder extends FieldMapper.Builder<DescriptorMapper.Builder, DescriptorMapper> {

        private String hash;

        protected Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        public Builder setHash(String hash) {
            this.hash = hash;
            return this;
        }

        @Override
        public DescriptorMapper build(BuilderContext context) {
//            setupFieldType(context);

            context.path().add(name);

            FieldMapper hashMapper = stringField(HASH).store(true).includeInAll(false).index(true).build(context);

            context.path().remove();

            MappedFieldType defaultFieldType = Defaults.FIELD_TYPE.clone();
            defaultFieldType.setNames(new MappedFieldType.Names(name));

            fieldType.setNames(new MappedFieldType.Names(name));

            return new DescriptorMapper(hash, hashMapper, name, fieldType, defaultFieldType, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DescriptorMapper.Builder builder = new DescriptorMapper.Builder(name);

            String hash = null;

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();

                if (HASH.equals(fieldName)) {
                    hash = (String) fieldNode;
                }
            }

            if (hash == null) {
                throw new ElasticsearchGenerationException("Hash not found");
            }

            builder.setHash(hash);

            return builder;
        }
    }

    private String hash;

    private FieldMapper hashMapper;

    public DescriptorMapper(String hash, FieldMapper hashMapper, String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
        this.hash = hash;
        this.hashMapper = hashMapper;
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();

        List<Double> values = new ArrayList<>();
        while (parser.currentToken() == XContentParser.Token.VALUE_NUMBER) {
            values.add(parser.doubleValue());
            parser.nextToken();
        }

        double[] descriptor = new double[values.size()];
        int i = 0;
        for (Double value : values) {
            descriptor[i++] = value;
        }

        if (descriptor == null) {
            throw new MapperParsingException("No descriptor is provided.");
        }

        byte[] content = SerializationUtils.toByteArray(descriptor);
        context.doc().add(new BinaryDocValuesField(name(), new BytesRef(content)));

        int[] value;
        if (hash.equals(HashEnum.BIT_SAMPLING.name())) {
            value = BitSampling.generateHashes(descriptor);
        } else if (hash.equals(HashEnum.LSH.name())) {
            value = LocalitySensitiveHashing.generateHashes(descriptor);
        } else {
            throw new IllegalArgumentException();
        }

        hashMapper.parse(context.createExternalValueContext(SerializationUtils.arrayToString(value)));

        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        return;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());

        builder.field("type", CONTENT_TYPE);
        builder.field(HASH, hash);

        return builder.endObject();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
