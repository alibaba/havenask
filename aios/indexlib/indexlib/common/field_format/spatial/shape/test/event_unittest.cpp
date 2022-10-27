#include "indexlib/common/field_format/spatial/shape/test/event_unittest.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(shape, EventTest);

EventTest::EventTest()
{
}

EventTest::~EventTest()
{
}

void EventTest::CaseSetUp()
{
}

void EventTest::CaseTearDown()
{
}

void EventTest::TestXYOrder()
{
    {
        Point p1(1, 1);
        Point p2(2, 1);
        ASSERT_EQ(-1, Event::xyorder(&p1, &p2));
        ASSERT_EQ(1, Event::xyorder(&p2, &p1));
    }

    {
        Point p1(1, 1);
        Point p2(1, 2);
        ASSERT_EQ(-1, Event::xyorder(&p1, &p2));
        ASSERT_EQ(1, Event::xyorder(&p2, &p1));
    }

    {
        Point p1(1, 1);
        Point p2(1, 1);
        ASSERT_EQ(0, Event::xyorder(&p1, &p2));
        ASSERT_EQ(0, Event::xyorder(&p2, &p1));
    }
}

IE_NAMESPACE_END(common);

