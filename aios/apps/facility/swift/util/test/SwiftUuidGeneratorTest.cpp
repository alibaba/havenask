#include "swift/util/SwiftUuidGenerator.h"

#include <iosfwd>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace util {

class SwiftUuidGeneratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftUuidGeneratorTest::setUp() {}

void SwiftUuidGeneratorTest::tearDown() {}

TEST_F(SwiftUuidGeneratorTest, testSimple) {
    {
        SwiftUuidGenerator generator;
        IdData idData;
        idData.id = generator.genRequestUuid(1000, 64, true, 10);
        ASSERT_TRUE(idData.requestId.logFlag);
        ASSERT_EQ(idData.requestId.timestampMs, 1000);
        ASSERT_EQ(idData.requestId.partid, 64);
        ASSERT_EQ(idData.requestId.seq, 10);
    }
    {
        SwiftUuidGenerator generator;
        IdData idData;
        idData.id = generator.genRequestUuid(1000, 261, true, 260);
        ASSERT_TRUE(idData.requestId.logFlag);
        ASSERT_EQ(idData.requestId.timestampMs, 1000);
        ASSERT_EQ(idData.requestId.partid, 5);
        ASSERT_EQ(idData.requestId.seq, 4);
    }
}

}; // namespace util
}; // namespace swift
