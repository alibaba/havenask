#ifndef __INDEXLIB_INMEMORYSEGMENTMODIFIERTEST_H
#define __INDEXLIB_INMEMORYSEGMENTMODIFIERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/segment/in_memory_segment_modifier.h"

IE_NAMESPACE_BEGIN(partition);

class InMemorySegmentModifierTest : public INDEXLIB_TESTBASE
{
public:
    InMemorySegmentModifierTest();
    ~InMemorySegmentModifierTest();

    DECLARE_CLASS_NAME(InMemorySegmentModifierTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestUpdateDocument();
    void TestUpdateEncodedFieldValue();
    void TestRemoveDocument();

private:
    autil::mem_pool::Pool mPool;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemorySegmentModifierTest, TestUpdateDocument);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentModifierTest, TestUpdateEncodedFieldValue);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentModifierTest, TestRemoveDocument);


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INMEMORYSEGMENTMODIFIERTEST_H
