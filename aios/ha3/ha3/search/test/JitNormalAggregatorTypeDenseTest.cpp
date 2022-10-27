#define TEST_CLASS_NAME JitNormalAggregatorTypeDenseTest
#define ENABLE_DENSE_MODE true
#define DefGroupType(KeyType, GroupTypeName)            \
    typedef typename DenseMapTraits<KeyType>::GroupMapType GroupTypeName;
#include "JitNormalAggregatorTest.cpp.inc"
