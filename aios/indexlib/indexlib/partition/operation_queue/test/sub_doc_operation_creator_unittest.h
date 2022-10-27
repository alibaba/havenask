#ifndef __INDEXLIB_SUBDOCOPERATIONCREATORTEST_H
#define __INDEXLIB_SUBDOCOPERATIONCREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

IE_NAMESPACE_BEGIN(partition);

class SubDocOperationCreatorTest : public INDEXLIB_TESTBASE
{
public:
    SubDocOperationCreatorTest();
    ~SubDocOperationCreatorTest();

    DECLARE_CLASS_NAME(SubDocOperationCreatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateNoOpearation();
    void TestCreateNoMainCreator();
    void TestCreateNoSubDoc();
    void TestCreateMainAndSub();

private:
    MockOperation* MakeOperation(int64_t ts, autil::mem_pool::Pool* pool);

private:
    autil::mem_pool::Pool mPool;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mSubSchema;
    document::NormalDocumentPtr mDoc;
    document::NormalDocumentPtr mSubDoc;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SubDocOperationCreatorTest, TestCreateNoOpearation);
INDEXLIB_UNIT_TEST_CASE(SubDocOperationCreatorTest, TestCreateNoSubDoc);
INDEXLIB_UNIT_TEST_CASE(SubDocOperationCreatorTest, TestCreateNoMainCreator);
INDEXLIB_UNIT_TEST_CASE(SubDocOperationCreatorTest, TestCreateMainAndSub);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SUBDOCOPERATIONCREATORTEST_H
