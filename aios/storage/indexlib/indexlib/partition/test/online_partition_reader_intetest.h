#ifndef __INDEXLIB_ONLINEPARTITIONREADERINTETEST_H
#define __INDEXLIB_ONLINEPARTITIONREADERINTETEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OnlinePartitionReaderInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    OnlinePartitionReaderInteTest();
    ~OnlinePartitionReaderInteTest();

    DECLARE_CLASS_NAME(OnlinePartitionReaderInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestOpenForIndex();
    void TestOpenForIndexWithSection();
    void TestOpenForCompressIndexWithSection();
    void TestOpenForPrimaryKey();
    void TestOpenForAttribute();
    void TestOpenForPackAttribute();
    void TestOpenForSummary();
    void TestOpenForSummaryWithPack();
    void TestOpenForVirtualAttribute(); // TODO

    void TestSubDocOpenForIndex();
    void TestSubDocOpenForIndexWithSection();
    void TestSubDocOpenForPrimaryKey();
    void TestSubDocOpenForAttribute();

    void TestGetSortedDocIdRanges(); // TODO
    void TestDisableIndex();
    void TestDisableSortField();
    void TestDisableSubIndex();
    void TestDisableSubIndexByConfig();
    void TestDisableSpatialIndexMode1();
    void TestDisableSpatialIndexMode2();

private:
    void AssertSectionLen(const IndexPartitionReaderPtr& reader, const std::string& indexName, docid_t docid,
                          section_len_t expectLen);

    void CheckReadSubAttribute(const index::PackAttributeReaderPtr& packAttrReader, const std::string& attrName,
                               docid_t docId, const std::string& expectedValue);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, OnlinePartitionReaderInteTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForIndexWithSection);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForCompressIndexWithSection);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForPrimaryKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForPackAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForSummary);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestOpenForSummaryWithPack);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestSubDocOpenForIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestSubDocOpenForIndexWithSection);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestSubDocOpenForPrimaryKey);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestSubDocOpenForAttribute);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestDisableIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestDisableSortField);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestDisableSubIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestDisableSubIndexByConfig);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestDisableSpatialIndexMode1);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlinePartitionReaderInteTest, TestDisableSpatialIndexMode2);
}} // namespace indexlib::partition

#endif //__INDEXLIB_ONLINEPARTITIONREADERINTETEST_H
