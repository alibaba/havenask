#ifndef __INDEXLIB_REMOVEOPERATIONCREATORTEST_H
#define __INDEXLIB_REMOVEOPERATIONCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class RemoveOperationCreatorTest : public INDEXLIB_TESTBASE
{
public:
    RemoveOperationCreatorTest();
    ~RemoveOperationCreatorTest();

    DECLARE_CLASS_NAME(RemoveOperationCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreate();

    // TODO: add uint128_t case
private:
    autil::mem_pool::Pool mPool;
    config::IndexPartitionSchemaPtr mSchema;
    document::NormalDocumentPtr mDoc;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RemoveOperationCreatorTest, TestCreate);
}} // namespace indexlib::partition

#endif //__INDEXLIB_REMOVEOPERATIONCREATORTEST_H
