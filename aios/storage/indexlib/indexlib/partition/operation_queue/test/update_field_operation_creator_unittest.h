#ifndef __INDEXLIB_UPDATEFIELDOPERATIONCREATORTEST_H
#define __INDEXLIB_UPDATEFIELDOPERATIONCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/partition/operation_queue/update_field_operation_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class UpdateFieldOperationCreatorTest : public INDEXLIB_TESTBASE
{
public:
    UpdateFieldOperationCreatorTest();
    ~UpdateFieldOperationCreatorTest();

    DECLARE_CLASS_NAME(UpdateFieldOperationCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreate();
    void TestCreateOperationItems();
    void TestUpdateIndex();
    void TestUpdateIndexAndAttr();

private:
    autil::mem_pool::Pool mPool;
    config::IndexPartitionSchemaPtr mSchema;
    document::NormalDocumentPtr mDoc;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationCreatorTest, TestCreate);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationCreatorTest, TestCreateOperationItems);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationCreatorTest, TestUpdateIndex);
INDEXLIB_UNIT_TEST_CASE(UpdateFieldOperationCreatorTest, TestUpdateIndexAndAttr);
}} // namespace indexlib::partition

#endif //__INDEXLIB_UPDATEFIELDOPERATIONCREATORTEST_H
