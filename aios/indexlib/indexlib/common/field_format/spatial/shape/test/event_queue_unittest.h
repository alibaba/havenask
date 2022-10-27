#ifndef __INDEXLIB_EVENTQUEUETEST_H
#define __INDEXLIB_EVENTQUEUETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/event_queue.h"

IE_NAMESPACE_BEGIN(common);

class EventQueueTest : public INDEXLIB_TESTBASE
{
public:
    EventQueueTest();
    ~EventQueueTest();

    DECLARE_CLASS_NAME(EventQueueTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EventQueueTest, TestSimpleProcess);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EVENTQUEUETEST_H
