#ifndef __INDEXLIB_COMPRESSRATIOCALCULATORTEST_H
#define __INDEXLIB_COMPRESSRATIOCALCULATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/segment/compress_ratio_calculator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class CompressRatioCalculatorTest : public INDEXLIB_TESTBASE
{
public:
    CompressRatioCalculatorTest();
    ~CompressRatioCalculatorTest();

    DECLARE_CLASS_NAME(CompressRatioCalculatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestGetRatioForKVTable();
    void TestGetRatioForKKVTable();

private:
    config::IndexPartitionSchemaPtr PrepareData(TableType tableType, size_t shardingCount, bool compress);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressRatioCalculatorTest, TestGetRatioForKVTable);
INDEXLIB_UNIT_TEST_CASE(CompressRatioCalculatorTest, TestGetRatioForKKVTable);
}} // namespace indexlib::partition

#endif //__INDEXLIB_COMPRESSRATIOCALCULATORTEST_H
