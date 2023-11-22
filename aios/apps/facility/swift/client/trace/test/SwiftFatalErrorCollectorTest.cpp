#include "swift/client/trace/SwiftFatalErrorCollector.h"

#include <deque>
#include <iosfwd>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace client {

class SwiftFatalErrorCollectorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void SwiftFatalErrorCollectorTest::setUp() {}

void SwiftFatalErrorCollectorTest::tearDown() {}

TEST_F(SwiftFatalErrorCollectorTest, testSimple) {
    auto *col_1 = ErrorCollectorSingleton::getInstance();
    col_1->addRequestHasErrorDataInfo("test");
    ASSERT_EQ(1, col_1->getTotalRequestFatalCount());
    auto fatalInfo = col_1->getLastestRequestFatalInfo();
    ASSERT_EQ(1, fatalInfo.size());
    ASSERT_EQ("test", fatalInfo[0]);

    auto *col_2 = ErrorCollectorSingleton::getInstance();
    ASSERT_TRUE(col_1 == col_2);
}

}; // namespace client
}; // namespace swift
