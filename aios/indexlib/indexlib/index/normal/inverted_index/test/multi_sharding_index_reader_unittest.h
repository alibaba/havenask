#ifndef __INDEXLIB_MULTISHARDINGINDEXREADERTEST_H
#define __INDEXLIB_MULTISHARDINGINDEXREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_sharding_index_reader.h"

IE_NAMESPACE_BEGIN(index);

class MultiShardingIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    MultiShardingIndexReaderTest();
    ~MultiShardingIndexReaderTest();

    DECLARE_CLASS_NAME(MultiShardingIndexReaderTest);
public:
    void TestSimpleProcess();
    void TestCreateKeyIterator();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MultiShardingIndexReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MultiShardingIndexReaderTest, TestCreateKeyIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTISHARDINGINDEXREADERTEST_H
