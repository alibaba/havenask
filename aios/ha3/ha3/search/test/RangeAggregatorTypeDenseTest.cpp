#define TEST_CLASS_NAME RangeAggregatorTypeDenseTest
#define ENABLE_DENSE_MODE true
#define DefGroupType(KeyType, GroupTypeName)            \
    typedef typename DenseMapTraits<KeyType>::GroupMapType GroupTypeName;
#include "RangeAggregatorTest.cpp.inc"
