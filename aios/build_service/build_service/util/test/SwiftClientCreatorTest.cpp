#include "build_service/test/unittest.h"
#include "build_service/util/SwiftClientCreator.h"
#include <autil/StringUtil.h>
#include <iostream>

using namespace std;
using namespace std::tr1;
using namespace std::tr1::placeholders;
using namespace autil;
using namespace testing;

namespace build_service {
namespace util {

class SwiftClientCreatorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(SwiftClientCreatorTest, testInitConfigStr) {
    string config1 = SwiftClientCreator::getInitConfigStr("zfs1", "");
    ASSERT_EQ("zkPath=zfs1", config1);
}
}
}
