#ifndef __INDEXLIB_ONLINEPARTITIONREADERINTETEST_H
#define __INDEXLIB_ONLINEPARTITIONREADERINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionReaderInteTest : public INDEXLIB_TESTBASE
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
    void TestOpenForPrimaryKey();
    void TestOpenForAttribute();
    void TestOpenForPackAttribute();
    void TestOpenForSummary();
    void TestOpenForSummaryWithPack();
    void TestOpenForVirtualAttribute();    // TODO

    void TestSubDocOpenForIndex();
    void TestSubDocOpenForIndexWithSection();
    void TestSubDocOpenForPrimaryKey();
    void TestSubDocOpenForAttribute();

    void TestGetSortedDocIdRanges();    // TODO
    void TestDisableIndex();
    void TestDisableSubIndex();
    void TestDisableSpatialIndexMode1();
    void TestDisableSpatialIndexMode2();
private:
    void AssertSectionLen(const IndexPartitionReaderPtr& reader, 
                          const std::string& indexName, 
                          docid_t docid,
                          section_len_t expectLen);
    
    void CheckReadSubAttribute(const index::PackAttributeReaderPtr& packAttrReader,
                               const std::string& attrName,
                               docid_t docId,
                               const std::string& expectedValue);
    
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestOpenForIndex);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestOpenForIndexWithSection);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestOpenForPrimaryKey);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestOpenForAttribute);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestOpenForPackAttribute);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestOpenForSummary);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestOpenForSummaryWithPack);


INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestSubDocOpenForIndex);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestSubDocOpenForIndexWithSection);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestSubDocOpenForPrimaryKey);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestSubDocOpenForAttribute);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestDisableIndex);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestDisableSubIndex);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestDisableSpatialIndexMode1);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReaderInteTest, TestDisableSpatialIndexMode2);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEPARTITIONREADERINTETEST_H
