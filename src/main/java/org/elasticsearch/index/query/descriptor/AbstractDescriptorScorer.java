package org.elasticsearch.index.query.descriptor;

import net.semanticmetadata.lire.imageanalysis.features.LireFeature;
import net.semanticmetadata.lire.utils.SerializationUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchImageProcessException;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Calculate score for each image
 * score = (1 / distance) * boost
 */
public abstract class AbstractDescriptorScorer extends Scorer {
    private final double[] descriptor;
    private final IndexReader reader;
    private final float boost;
    private BinaryDocValues binaryDocValues;
    private String fieldName;

    protected AbstractDescriptorScorer(Weight weight, double[] descriptor, IndexReader reader, float boost, String fieldName) {
        super(weight);
        this.descriptor = descriptor;
        this.reader = reader;
        this.boost = boost;
        this.fieldName = fieldName;
    }

    @Override
    public float score() throws IOException {
        assert docID() != NO_MORE_DOCS;

        if (binaryDocValues == null) {
            LeafReader atomicReader = (LeafReader) reader;
            binaryDocValues = atomicReader.getBinaryDocValues(fieldName);
        }

        try {
            BytesRef bytesRef = binaryDocValues.get(docID());
            double[] descriptor2 = SerializationUtils.toDoubleArray(bytesRef.bytes);

            double distance = getDistance(descriptor, descriptor2);
            double score;
            if (Double.compare(distance, 1.0f) <= 0) { // distance less than 1, consider as same image
                score = 2f - distance;
            } else {
                score = 1 / distance;
            }
            return (float)score * boost;
        } catch (Exception e) {
            throw new ElasticsearchImageProcessException("Failed to calculate score", e);
        }
    }

    public static double getDistance(double[] d1, double[] d2) {
        assert d1.length == d2.length;

        double distance = 0;
        for (int i = 0; i < d1.length; i++) {
            double diff = d1[i] - d2[i];
            distance += diff * diff;
        }

        return distance;
    }

    @Override
    public int freq() {
        return 1;
    }
}
