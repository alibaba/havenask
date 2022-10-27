#ifndef __INDEXLIB_EVENTTEST_H
#define __INDEXLIB_EVENTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/spatial/shape/event.h"

IE_NAMESPACE_BEGIN(common);

class EventTest : public INDEXLIB_TESTBASE
{
public:
    EventTest();
    ~EventTest();

    DECLARE_CLASS_NAME(EventTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestXYOrder();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EventTest, TestXYOrder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EVENTTEST_H
