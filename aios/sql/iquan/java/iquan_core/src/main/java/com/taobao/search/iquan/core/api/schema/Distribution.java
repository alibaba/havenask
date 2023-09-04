package com.taobao.search.iquan.core.api.schema;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class Distribution implements RelDistribution {
    private static final Logger logger = LoggerFactory.getLogger(Distribution.class);

    private int partitionCnt = 0;
    //index maybe not equal pos, so need record pos
    private List<MutablePair<Integer, String>> posHashFields = new ArrayList<>();
    private HashType hashFunction = HashType.HF_INVALID_HASH;
    private Map<String, String> hashParams = new HashMap<>();
    private Map<String, String> partFixKeyMap = new TreeMap<>();
    //equalHashFields maybe empty or must contain all posHashFields T1 value
    //equalHashFields index must be equal pos, so not need record pos
    //equalHashFields only exist on Type.HASH_DISTRIBUTED
    private List<List<String>> equalHashFields = new ArrayList<>();
    private Type type;
    private ImmutableIntList keys;

    Distribution() {
    }

    public Distribution(Type type, ImmutableIntList keys) {
        this.type = type;
        this.keys = keys;
    }

    public static final ImmutableIntList EMPTY = ImmutableIntList.of();
    public static final Distribution DEFAULT = new Builder(Type.ANY, EMPTY).build();
    /**
     * SINGLETON is all records appear in only one partition.
     * BROADCAST is all records appear in each partition.
     * RANGE is ROUTING_HASH but only has the first hash field.
     * if location partitionCnt == 1, any distribution type is actual equal.
     * so qrs、single node, we can use any of (SINGLETON、BROADCAST、RANGE、HASH、RANDOM), according to code implement.
     * */
    public static final Distribution SINGLETON = new Builder(Type.SINGLETON, EMPTY).partitionCnt(1).build();
    public static final Distribution BROADCAST = new Builder(Type.BROADCAST_DISTRIBUTED, EMPTY).build();
    public static final Distribution RANGE = new Builder(Type.RANGE_DISTRIBUTED, EMPTY).build();
    //public static final Distribution RANDOM = new Builder(Type.RANDOM_DISTRIBUTED, EMPTY).build();

    //copy when need change distribution
    public Distribution copy() {
        Distribution distribution = new Distribution(type, keys);
        distribution.setPartitionCnt(partitionCnt);
        distribution.copyPosHashFields(posHashFields);
        distribution.setHashFunction(hashFunction.getName());
        distribution.getHashParams().putAll(hashParams);
        distribution.getPartFixKeyMap().putAll(partFixKeyMap);
        distribution.copyEqualHashFields(equalHashFields);
        return distribution;
    }

    public int indexOfHashField(String field) {
        for (Pair<Integer, String> pair : posHashFields) {
            if (pair.getRight().equals(field)) {
                return pair.getLeft();
            }
        }
        return -1;
    }

    public int getPartitionCnt() {
        return partitionCnt;
    }

    public void setPartitionCnt(int partitionCnt) {
        this.partitionCnt = partitionCnt;
    }

    public void setType(RelDistribution.Type type) {
        this.type = type;
    }

    private void copyPosHashFields(List<MutablePair<Integer, String>> posHashFields) {
        //only private use
        for (int i = 0; i < posHashFields.size(); i++) {
            Pair<Integer, String> pair = posHashFields.get(i);
            this.posHashFields.add(MutablePair.of(pair.getLeft(), pair.getRight()));
        }
    }

    private void copyEqualHashFields(List<List<String>> equalHashFields) {
        if (equalHashFields == null || equalHashFields.isEmpty()) {
            return;
        }
        int i = 0;
        while(i < equalHashFields.size() && i < this.equalHashFields.size()) {
            this.equalHashFields.get(i).addAll(equalHashFields.get(i++));
        }
        while(i < equalHashFields.size()) {
            this.equalHashFields.add(new ArrayList<>(equalHashFields.get(i++)));
        }
    }

    public List<List<String>> getEqualHashFields() {
        return equalHashFields;
    }

    public void setEqualHashFields(List<List<String>> equalHashFields) {
        this.equalHashFields = equalHashFields;
    }

    public List<String> getHashFields() {
        return posHashFields.stream().map(Pair::getRight).collect(Collectors.toList());
    }

    public void setHashFields(List<String> hashFields) {
        posHashFields.clear();
        if (hashFields == null) {
            return;
        }
        for(int i = 0; i < hashFields.size(); i++) {
            posHashFields.add(MutablePair.of(i, hashFields.get(i)));
        }
    }

    public List<MutablePair<Integer, String>> getPosHashFields() {
        return posHashFields;
    }

    public void setPosHashFields(List<MutablePair<Integer, String>> posHashFields) {
        this.posHashFields = posHashFields;
    }

    public HashType getHashFunction() {
        return hashFunction;
    }

    public void setHashFunction(String hashFunction) {
        this.hashFunction = HashType.from(hashFunction);
    }

    public Map<String, String> getHashParams() {
        return hashParams;
    }

    public void setHashParams(Map<String, String> hashParams) {
        this.hashParams = hashParams;
    }

    public Map<String, String> getPartFixKeyMap() {
        return partFixKeyMap;
    }

    public void setPartFixKeyMap(Map<String, String> partFixKeyMap) {
        this.partFixKeyMap = partFixKeyMap;
    }

    public boolean isValid() {
        return partitionCnt > 1
                && posHashFields != null
                && hashFunction != null && hashFunction.isValid()
                && hashParams != null
                && (type == Type.HASH_DISTRIBUTED)
                || (partitionCnt == 1) && (type == Type.SINGLETON)
                || type == Type.RANDOM_DISTRIBUTED
                || type == Type.RANGE_DISTRIBUTED
                || type == Type.BROADCAST_DISTRIBUTED
                || type == Type.ANY;
    }

    public String getDigest() {
        return String.format("partitionCnt=%d, hashFunction=%s, hashFields=%s, hashParams=%s, distributionType=%s",
                partitionCnt, hashFunction.getName(), posHashFields, hashParams, type);
    }

    @Override
    public RelTraitDef getTraitDef() {
        return IquanDistributionTraitDef.INSTANCE;
    }

    @Override
    public boolean satisfies(RelTrait trait) {
        return false;
    }

    @Override
    public String toString() {
        return getDigest();
    }

    @Override
    public void register(RelOptPlanner planner) {

    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Type type, ImmutableIntList keys) {
        return new Builder(type, keys);
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public List<Integer> getKeys() {
        return keys;
    }

    @Override
    public Distribution apply(Mappings.TargetMapping mapping) {
        return Distribution.DEFAULT;
    }

    @Override
    public boolean isTop() {
        return type == Type.ANY;
    }

    @Override
    public int compareTo(RelMultipleTrait o) {
        throw new RuntimeException("not support");
    }

    public static class Builder {
        private Distribution distribution;

        public Builder() {
            distribution = new Distribution();
        }

        public Builder(Type type, ImmutableIntList keys) {
            distribution = new Distribution(type, keys);
        }

        public Builder partitionCnt(int partitionCnt) {
            distribution.setPartitionCnt(partitionCnt);
            return this;
        }

        public Builder hashFields(List<String> hashFields) {
            distribution.setHashFields(hashFields);
            return this;
        }

        public Builder hashFunction(String hashFunction) {
            distribution.setHashFunction(hashFunction);
            return this;
        }

        public Builder hashParams(Map<String, String> hashParams) {
            distribution.setHashParams(hashParams);
            return this;
        }

        public Distribution build() {
            if (distribution.isValid()) {
                return distribution;
            }
            logger.error("distribution is not valid, {}", distribution.getDigest());
            return null;
        }
    }
}

class IquanDistributionTraitDef extends RelTraitDef<Distribution> {
    public static final IquanDistributionTraitDef INSTANCE = new IquanDistributionTraitDef();

    private IquanDistributionTraitDef() {
    }

    public Class<Distribution> getTraitClass() {
        return Distribution.class;
    }

    public String getSimpleName() {
        return this.getClass().getSimpleName();
    }

    public RelNode convert(RelOptPlanner planner, RelNode rel, Distribution toTrait, boolean allowInfiniteCostConverters) {
        throw new RuntimeException("not support");
    }

    public boolean canConvert(RelOptPlanner planner, Distribution fromTrait, Distribution toTrait) {
        return true;
    }

    public Distribution getDefault() {
        return Distribution.DEFAULT;
    }
}
