#include "ha3/proto/ProtoClassComparator.h"

#include "ha3/proto/BasicDefs.pb.h"
#include "unittest/unittest.h"

namespace isearch {
namespace proto {

class ProtoClassComparatorTest : public TESTBASE {
public:
    ProtoClassComparatorTest();
    ~ProtoClassComparatorTest();

public:
    void setUp();
    void tearDown();

protected:
};

ProtoClassComparatorTest::ProtoClassComparatorTest() {}

ProtoClassComparatorTest::~ProtoClassComparatorTest() {}

void ProtoClassComparatorTest::setUp() {}

void ProtoClassComparatorTest::tearDown() {}

TEST_F(ProtoClassComparatorTest, testEqual) {

    Range lhs;
    Range rhs;
    ASSERT_TRUE(lhs == rhs);
    lhs.set_from(1);
    ASSERT_TRUE(lhs != rhs);
    rhs.set_from(1);
    ASSERT_TRUE(lhs == rhs);
    rhs.set_from(0);
    ASSERT_TRUE(lhs != rhs);
    rhs.set_from(2);
    ASSERT_TRUE(lhs != rhs);

    lhs.set_from(1);
    rhs.set_from(1);

    lhs.set_to(1);
    ASSERT_TRUE(lhs != rhs);
    rhs.set_to(1);
    ASSERT_TRUE(lhs == rhs);
    rhs.set_to(0);
    ASSERT_TRUE(lhs != rhs);
    rhs.set_to(2);
    ASSERT_TRUE(lhs != rhs);

    lhs.set_to(1);
    rhs.set_to(1);

    lhs.clear_from();
    ASSERT_TRUE(lhs != rhs);
    rhs.clear_from();
    ASSERT_TRUE(lhs == rhs);
}

TEST_F(ProtoClassComparatorTest, testLess) {

    Range lhs;
    Range rhs;
    ASSERT_TRUE(!(rhs < lhs));
    ASSERT_TRUE(!(lhs < rhs));
    lhs.set_from(1);
    ASSERT_TRUE(rhs < lhs);
    rhs.set_from(1);
    ASSERT_TRUE(!(rhs < lhs));
    ASSERT_TRUE(!(lhs < rhs));
    rhs.set_from(0);
    ASSERT_TRUE(rhs < lhs);
    rhs.set_from(2);
    ASSERT_TRUE(lhs < rhs);

    lhs.set_from(1);
    rhs.set_from(1);

    lhs.set_to(1);
    ASSERT_TRUE(rhs < lhs);
    rhs.set_to(1);
    ASSERT_TRUE(!(rhs < lhs));
    ASSERT_TRUE(!(lhs < rhs));
    rhs.set_to(0);
    ASSERT_TRUE(rhs < lhs);
    rhs.set_to(2);
    ASSERT_TRUE(lhs < rhs);

    lhs.set_to(1);
    rhs.set_to(1);

    lhs.clear_from();
    ASSERT_TRUE(lhs < rhs);
    rhs.clear_from();
    ASSERT_TRUE(!(rhs < lhs));
    ASSERT_TRUE(!(lhs < rhs));
}

} // namespace proto
} // namespace isearch
