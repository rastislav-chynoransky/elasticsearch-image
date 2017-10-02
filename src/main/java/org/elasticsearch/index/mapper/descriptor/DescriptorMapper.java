package org.elasticsearch.index.mapper.descriptor;

import net.semanticmetadata.lire.indexers.hashing.BitSampling;
import net.semanticmetadata.lire.indexers.hashing.LocalitySensitiveHashing;
import net.semanticmetadata.lire.utils.SerializationUtils;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
        public static final MappedFieldType FIELD_TYPE = new DescriptorMapper.DescriptorFieldType();

        static {
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

        protected Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            this.builder = this;
        }

        @Override
        public DescriptorMapper build(BuilderContext context) {
            setupFieldType(context);
            return new DescriptorMapper(name, fieldType, defaultFieldType, context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            DescriptorMapper.Builder builder = new DescriptorMapper.Builder(name);

            return builder;
        }
    }

    protected DescriptorMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();

        assert parser.currentToken() == XContentParser.Token.START_ARRAY;

        List<Double> values = new ArrayList<>();
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            values.add(parser.doubleValue());
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
        context.doc().add(new BinaryDocValuesField("descriptor", new BytesRef(content)));

        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<Field> fields) throws IOException {
        return;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
