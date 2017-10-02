package org.elasticsearch.index.query.descriptor;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DescriptorHashQuery extends Query {

    private final Term term;
    private double[] descriptor;
    private String fieldName;
    private DescriptorScoreCache descriptorScoreCache;

    public DescriptorHashQuery(Term t, double[] descriptor, float boost, String fieldName, DescriptorScoreCache descriptorScoreCache) {
        this.term = t;
        this.descriptor = descriptor;
        this.fieldName = fieldName;
        this.descriptorScoreCache = descriptorScoreCache;
        setBoost(boost);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {

        final IndexReaderContext context = searcher.getTopReaderContext();
        final TermContext termState = TermContext.build(context, term);

        return new DescriptorHashWeight(termState);
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        buffer.append(ToStringUtils.boost(getBoost()));
        return buffer.toString();
    }

    final class DescriptorHashWeight extends Weight
    {
        private final TermContext termStates;

        public DescriptorHashWeight(TermContext termStates) throws IOException {
            super(DescriptorHashQuery.this);
            assert termStates != null : "TermContext must not be null";
            this.termStates = termStates;
        }

        @Override
        public String toString() { return "weight(" + DescriptorHashQuery.this + ")"; }

        @Override
        public float getValueForNormalization() {
            return 1f;
        }

        @Override
        public void normalize(float queryNorm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            assert termStates.topReaderContext == ReaderUtil.getTopLevelContext(context) : "The top-reader used to create Weight (" + termStates.topReaderContext + ") is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
            final TermsEnum termsEnum = getTermsEnum(context);
            if (termsEnum == null) {
                return null;
            }
            PostingsEnum docs = termsEnum.postings( null);
            assert docs != null;
            return new DescriptorHashScorer(this, docs, context.reader());
        }

        private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
            final TermState state = termStates.get(context.ord);
            if (state == null) { // term is not present in that reader
                assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
                return null;
            }
            final TermsEnum termsEnum = context.reader().terms(term.field()).iterator();
            termsEnum.seekExact(term.bytes(), state);
            return termsEnum;
        }

        private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
            return reader.docFreq(term) == 0;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Scorer scorer = scorer(context);
            boolean exists = (scorer != null && scorer.iterator().advance(doc) == doc);

            if (exists) {
                float score = scorer.score();
                List<Explanation> details=new ArrayList<>();
                if (getBoost() != 1.0f) {
                    details.add(Explanation.match(getBoost(), "boost"));
                    score = score / getBoost();
                }
                details.add(Explanation.match(score ,"image score (1/distance)"));
                return Explanation.match(score, DescriptorHashWeight.this.toString() + ", product of:",details);

            } else{
                return Explanation.noMatch(DescriptorHashWeight.this.toString() + " doesn't match id " + doc);
            }
        }

        @Override
        public void extractTerms(Set<Term> terms) {

        }
    }

    final class DescriptorHashScorer extends AbstractDescriptorScorer
    {
        private final PostingsEnum docsEnum;
        private final IndexReader reader;

        DescriptorHashScorer(Weight weight, PostingsEnum td, IndexReader reader) {
            super(weight, descriptor, reader, DescriptorHashQuery.this.getBoost(), fieldName);
            this.docsEnum = td;
            this.reader = reader;
        }

        @Override
        public int docID() {
            return docsEnum.docID();
        }

        @Override
        public float score() throws IOException {
            assert docID() != DocIdSetIterator.NO_MORE_DOCS;
            int docId = docID();
            String cacheKey = reader.toString() + ":" + docId;
            if (descriptorScoreCache.getScore(cacheKey) != null) {
                return 0f;  // BooleanScorer will add all score together, return 0 for docs already processed
            }
            float score = super.score();
            descriptorScoreCache.setScore(cacheKey, score);
            return score;
        }

        @Override
        public DocIdSetIterator iterator() {
            // added for lucene 5.5.0
            return new DocIdSetIterator() {

                @Override
                public int docID() {
                    return docsEnum.docID();
                }

                @Override
                public int nextDoc() throws IOException {
                    return docsEnum.nextDoc();
                }

                @Override
                public int advance(int target) throws IOException {
                    return docsEnum.advance(target);
                }

                @Override
                public long cost() {
                    return docsEnum.cost();
                }
            };
        }
    }
}
