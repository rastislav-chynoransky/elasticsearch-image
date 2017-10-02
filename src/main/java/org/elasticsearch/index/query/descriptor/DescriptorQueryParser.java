package org.elasticsearch.index.query.descriptor;

import net.semanticmetadata.lire.indexers.hashing.BitSampling;
import net.semanticmetadata.lire.indexers.hashing.LocalitySensitiveHashing;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.image.HashEnum;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DescriptorQueryParser implements QueryParser {

    public static final String NAME = "descriptor";

    @Override
    public String[] names() {
        return new String[] {NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();

        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext, "[descriptor] query malformed, no field");
        }


        String fieldName = parser.currentName();

        HashEnum hash = null;
        double[] descriptor = null;
        float boost = 1.0f;

        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    try {
                        if ("descriptor".equals(currentFieldName)) {
                            assert parser.currentToken() == XContentParser.Token.START_ARRAY;

                            List<Double> values = new ArrayList<>();
                            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                values.add(parser.doubleValue());
                            }

                            descriptor = new double[values.size()];
                            int i = 0;
                            for (Double value : values) {
                                descriptor[i++] = value;
                            }
                        } else if ("hash".equals(currentFieldName)) {
                            hash = HashEnum.getByName(parser.text());
                        } else if ("boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new QueryParsingException(parseContext, "[descriptor] query does not support [" + currentFieldName + "]");
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            }
            parser.nextToken();
        }

        if (hash == null)
            throw new QueryParsingException(parseContext, "No hash found");

        int[] value;

        if (hash.equals(HashEnum.BIT_SAMPLING)) {
            value = BitSampling.generateHashes(descriptor);
        } else if (hash.equals(HashEnum.LSH)) {
            value = LocalitySensitiveHashing.generateHashes(descriptor);
        } else {
            throw new IllegalArgumentException();
        }

        BooleanQuery.Builder builder = new BooleanQuery.Builder().setDisableCoord(true);

        for (int v : value) {
            builder.add(new BooleanClause(new DescriptorHashQuery(new Term(fieldName, Integer.toString(v)), descriptor, boost), BooleanClause.Occur.SHOULD));
        }

        return builder.build();
    }
}
