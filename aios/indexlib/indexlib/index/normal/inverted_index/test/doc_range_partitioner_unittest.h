#ifndef __INDEXLIB_DOCRANGEPARTITIONERTEST_H
#define __INDEXLIB_DOCRANGEPARTITIONERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_range_partitioner.h"
#include "indexlib/index/test/partition_info_creator.h"


IE_NAMESPACE_BEGIN(index);

class DocRangePartitionerTest : public INDEXLIB_TESTBASE
{
public:
    DocRangePartitionerTest();
    ~DocRangePartitionerTest();

    DECLARE_CLASS_NAME(DocRangePartitionerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestBatch();

private:
    std::string mRootDir;
    PartitionInfoCreatorPtr mPartInfoCreator;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocRangePartitionerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DocRangePartitionerTest, TestBatch);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOCRANGEPARTITIONERTEST_H
