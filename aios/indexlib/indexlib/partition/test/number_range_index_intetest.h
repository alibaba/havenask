#ifndef __INDEXLIB_NUMBERRANGEINDEXINTETEST_H
#define __INDEXLIB_NUMBERRANGEINDEXINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"
#include "indexlib/partition/index_partition_reader.h"

IE_NAMESPACE_BEGIN(partition);

class NumberRangeIndexInteTest : public INDEXLIB_TESTBASE
{
public:
    NumberRangeIndexInteTest();
    ~NumberRangeIndexInteTest();

    DECLARE_CLASS_NAME(NumberRangeIndexInteTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestInterval();
    void TestFieldType();
    void TestRecoverFailed();
    void TestDisableRange();

private:
    void InnerTestSeekDocCount(const IndexPartitionReaderPtr& reader);
    void InnerTestInterval(const std::string& fullDocString,
                           const std::string& rtDocString,
                           const std::string& incDcoString,
                           const std::vector<std::string>& queryStrings,
                           const std::vector<std::string>& resultStrings);
    index::SeekAndFilterIterator* CreateRangeIteratorWithSeek(const IndexPartitionReaderPtr& reader,
                                                             int64_t left, int64_t right);
private:
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NumberRangeIndexInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(NumberRangeIndexInteTest, TestInterval);
INDEXLIB_UNIT_TEST_CASE(NumberRangeIndexInteTest, TestFieldType);
INDEXLIB_UNIT_TEST_CASE(NumberRangeIndexInteTest, TestRecoverFailed);
INDEXLIB_UNIT_TEST_CASE(NumberRangeIndexInteTest, TestDisableRange);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_NUMBERRANGEINDEXINTETEST_H
