#ifndef __INDEXLIB_DENSEHASHTABLETEST_H
#define __INDEXLIB_DENSEHASHTABLETEST_H

#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common_define.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

namespace indexlib { namespace common {

class DenseHashTableTest : public INDEXLIB_TESTBASE
{
public:
    DenseHashTableTest() {}
    ~DenseHashTableTest() {}

    DECLARE_CLASS_NAME(DenseHashTableTest);

public:
    void CaseSetUp() override
    {
        std::string loadConfigStr = R"(
    {
        "load_config":
        [
            {
                "file_patterns" : [".*"],
                "load_strategy" : "cache",
                "load_strategy_param": {
                    "direct_io":false
                }
            }
    ]})";
        config::LoadConfigList loadConfigList;
        autil::legacy::FromJsonString(loadConfigList, loadConfigStr);
        loadConfigList.Check();
        RESET_FILE_SYSTEM(loadConfigList);
        GET_PARTITION_DIRECTORY()->MakeDirectory("test_segment");
        mCounter.Reset();
    }
    void CaseTearDown() override { GET_PARTITION_DIRECTORY()->RemoveDirectory("test_segment"); }

public:
    void TestSpecialKeyBucketInsert();
    void TestSpecialKeyBucketDelete();
    void TestSpecialKeyBucketForSpecialKey();
    void TestSpecialKeyBucketFull();
    void TestSpecialKeyBucket();
    void TestSpecialKeyBucketInteTest();
    void TestTimestamp0BucketInteTest();

    void TestTimestampBucket();
    void TestTimestampBucketShrink();
    void TestTimestampBucketInteTest();

    void TestOffsetBucket();
    void TestOffsetBucketSpecial();
    void TestOffsetBucketInteTest();

    void TestSimpleSpecialValueIterator();

    void TestOccupancyPct();

    void TestCompress();
    void TestCalculateDeleteCount();

    void TestInitNoIO();

private:
    util::BlockAccessCounter mCounter;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestSpecialKeyBucketInsert);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestSpecialKeyBucketDelete);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestSpecialKeyBucketForSpecialKey);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestSpecialKeyBucket);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestSpecialKeyBucketInteTest);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestTimestamp0BucketInteTest);

INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestTimestampBucket);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestTimestampBucketShrink);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestTimestampBucketInteTest);

INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestOffsetBucket);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestOffsetBucketSpecial);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestOffsetBucketInteTest);

INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestSpecialKeyBucketFull);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestSimpleSpecialValueIterator);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestOccupancyPct);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestCompress);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestCalculateDeleteCount);
INDEXLIB_UNIT_TEST_CASE(DenseHashTableTest, TestInitNoIO);
}} // namespace indexlib::common

#endif //__INDEXLIB_DENSEHASHTABLETEST_H
