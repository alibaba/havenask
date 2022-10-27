#define TEST_CLASS_NAME RangeAggregatorTypeUnorderedTest
#define ENABLE_DENSE_MODE false
#define DefGroupType(KeyType, GroupTypeName)                            \
    typedef typename UnorderedMapTraits<KeyType>::GroupMapType GroupTypeName;
#include "RangeAggregatorTest.cpp.inc"
