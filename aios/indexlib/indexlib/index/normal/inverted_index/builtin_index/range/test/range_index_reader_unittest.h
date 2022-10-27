#ifndef __INDEXLIB_RANGEINDEXREADERTEST_H
#define __INDEXLIB_RANGEINDEXREADERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_reader.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(index);

class RangeIndexReaderTest : public INDEXLIB_TESTBASE
{
public:
    RangeIndexReaderTest();
    ~RangeIndexReaderTest();

    DECLARE_CLASS_NAME(RangeIndexReaderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestRangeIndexReaderTermIllegal();
private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RangeIndexReaderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(RangeIndexReaderTest, TestRangeIndexReaderTermIllegal);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_RANGEINDEXREADERTEST_H
