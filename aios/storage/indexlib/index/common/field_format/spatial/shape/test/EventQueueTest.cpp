#include "indexlib/index/common/field_format/spatial/shape/EventQueue.h"

#include "unittest/unittest.h"

namespace indexlib::index {
class EventQueueTest : public TESTBASE
{
    EventQueueTest() = default;
    ~EventQueueTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(EventQueueTest, TestXYOrder)
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
        ASSERT_TRUE(1 == event->_edge || 2 == event->_edge);
        ASSERT_EQ(SEG_SIDE::LEFT, event->_type);

        eV = event->_point;
        ASSERT_TRUE(nullptr != eV);
        ASSERT_EQ(*eV, p[2]);

        ASSERT_TRUE(nullptr == event->_slseg);

        otherEnd = event->_otherEnd;
        ASSERT_TRUE(nullptr != otherEnd);
        ASSERT_EQ(event->_edge, otherEnd->_edge);
        ASSERT_EQ(SEG_SIDE::RIGHT, otherEnd->_type);
        if (1 == event->_edge) {
            ASSERT_TRUE(*(otherEnd->_point) == p[1]);
        } else {
            ASSERT_TRUE(*(otherEnd->_point) == p[0]);
        }
        ASSERT_TRUE(nullptr == otherEnd->_slseg);
        ASSERT_EQ(event, otherEnd->_otherEnd);
    }

    // event 1
    {
        event = eventQueue.Next();
        ASSERT_TRUE(nullptr != event);
        ASSERT_TRUE(1 == event->_edge || 2 == event->_edge);
        ASSERT_EQ(SEG_SIDE::LEFT, event->_type);

        eV = event->_point;
        ASSERT_TRUE(nullptr != eV);
        ASSERT_EQ(*eV, p[2]);

        ASSERT_TRUE(nullptr == event->_slseg);

        otherEnd = event->_otherEnd;
        ASSERT_TRUE(nullptr != otherEnd);
        ASSERT_EQ(event->_edge, otherEnd->_edge);
        ASSERT_EQ(SEG_SIDE::RIGHT, otherEnd->_type);
        if (1 == event->_edge) {
            ASSERT_TRUE(*(otherEnd->_point) == p[1]);
        } else {
            ASSERT_TRUE(*(otherEnd->_point) == p[0]);
        }
        ASSERT_TRUE(nullptr == otherEnd->_slseg);
        ASSERT_EQ(event, otherEnd->_otherEnd);
    }
}
} // namespace indexlib::index
