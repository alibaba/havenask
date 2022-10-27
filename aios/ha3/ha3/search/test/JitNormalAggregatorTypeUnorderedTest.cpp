#define TEST_CLASS_NAME JitNormalAggregatorTypeUnorderedTest
#define ENABLE_DENSE_MODE false
#define DefGroupType(KeyType, GroupTypeName)                            \
    typedef typename UnorderedMapTraits<KeyType>::GroupMapType GroupTypeName;
#include "JitNormalAggregatorTest.cpp.inc"
