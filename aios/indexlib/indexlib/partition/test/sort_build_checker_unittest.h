#ifndef __INDEXLIB_SORTBUILDCHECKERTEST_H
#define __INDEXLIB_SORTBUILDCHECKERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/sort_build_checker.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

IE_NAMESPACE_BEGIN(partition);

class SortBuildCheckerTest : public INDEXLIB_TESTBASE
{
public:
    SortBuildCheckerTest();
    ~SortBuildCheckerTest();

    DECLARE_CLASS_NAME(SortBuildCheckerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckSortSequence(
            const std::vector<document::NormalDocumentPtr>& docs,
            const std::string& docIdxs,
            const std::string& failDocPk);

private:
    config::IndexPartitionSchemaPtr mSchema;
    index_base::PartitionMetaPtr mPartitionMeta;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SortBuildCheckerTest, TestSimpleProcess);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SORTBUILDCHECKERTEST_H
