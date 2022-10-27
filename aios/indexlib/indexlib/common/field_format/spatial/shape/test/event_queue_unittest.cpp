#include "indexlib/common/field_format/spatial/shape/test/event_queue_unittest.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, EventQueueTest);

EventQueueTest::EventQueueTest()
{
}

EventQueueTest::~EventQueueTest()
{
}

void EventQueueTest::CaseSetUp()
{
}

void EventQueueTest::CaseTearDown()
{
}

void EventQueueTest::TestSimpleProcess()
{
    std::vector<Point> p;
    p.push_back(Point(3, 2));
    p.push_back(Point(2, 2));
    p.push_back(Point(1, 1));

    EventQueue eventQueue(p);

    Event* event;
    
    Point* eV;
    Event* otherEnd;

    // event 0
    {
        event = eventQueue.Next();
        ASSERT_TRUE(nullptr != event);
        ASSERT_TRUE(1 == event->edge || 2 == event->edge);
        ASSERT_EQ(LEFT, event->type);

        eV = event->eV;
        ASSERT_TRUE(nullptr != eV);
        ASSERT_EQ(*eV, p[2]);

        ASSERT_TRUE(nullptr == event->seg);

        otherEnd = event->otherEnd;
        ASSERT_TRUE(nullptr != otherEnd);
        ASSERT_EQ(event->edge, otherEnd->edge);
        ASSERT_EQ(RIGHT, otherEnd->type);
        if (1 == event->edge)
        {
            ASSERT_TRUE(*(otherEnd->eV) == p[1]);
        }
        else
        {
            ASSERT_TRUE(*(otherEnd->eV) == p[0]);
        }
        ASSERT_TRUE(nullptr == otherEnd->seg);
        ASSERT_EQ(event, otherEnd->otherEnd);
    }

    // event 1
    {
        event = eventQueue.Next();
        ASSERT_TRUE(nullptr != event);
        ASSERT_TRUE(1 == event->edge || 2 == event->edge);
        ASSERT_EQ(LEFT, event->type);

        eV = event->eV;
        ASSERT_TRUE(nullptr != eV);
        ASSERT_EQ(*eV, p[2]);

        ASSERT_TRUE(nullptr == event->seg);

        otherEnd = event->otherEnd;
        ASSERT_TRUE(nullptr != otherEnd);
        ASSERT_EQ(event->edge, otherEnd->edge);
        ASSERT_EQ(RIGHT, otherEnd->type);
        if (1 == event->edge)
        {
            ASSERT_TRUE(*(otherEnd->eV) == p[1]);
        }
        else
        {
            ASSERT_TRUE(*(otherEnd->eV) == p[0]);
        }
        ASSERT_TRUE(nullptr == otherEnd->seg);
        ASSERT_EQ(event, otherEnd->otherEnd);
    }

    // TODO other events
}

IE_NAMESPACE_END(common);

