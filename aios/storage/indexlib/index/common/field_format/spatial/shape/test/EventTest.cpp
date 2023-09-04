#include "indexlib/index/common/field_format/spatial/shape/Event.h"

#include "unittest/unittest.h"

namespace indexlib::index {
class EventTest : public TESTBASE
{
    EventTest() = default;
    ~EventTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(EventTest, TestXYOrder)
{
    {
        Point p1(1, 1);
        Point p2(2, 1);
        ASSERT_EQ(-1, Event::XYOrder(&p1, &p2));
        ASSERT_EQ(1, Event::XYOrder(&p2, &p1));
    }

    {
        Point p1(1, 1);
        Point p2(1, 2);
        ASSERT_EQ(-1, Event::XYOrder(&p1, &p2));
        ASSERT_EQ(1, Event::XYOrder(&p2, &p1));
    }

    {
        Point p1(1, 1);
        Point p2(1, 1);
        ASSERT_EQ(0, Event::XYOrder(&p1, &p2));
        ASSERT_EQ(0, Event::XYOrder(&p2, &p1));
    }
}
} // namespace indexlib::index
