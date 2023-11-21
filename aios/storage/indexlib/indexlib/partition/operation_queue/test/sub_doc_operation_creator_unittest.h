#pragma once

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/operation_queue/sub_doc_operation_creator.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

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
}} // namespace indexlib::partition
