#include "autil/EnvUtil.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "suez/service/DdlServiceImpl.h"
#include "suez/service/LeaderRouter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class DdlServiceTest : public TESTBASE {};

TEST_F(DdlServiceTest, getLeaderRouterTest) {
    DdlServiceImpl impl;
    LeaderRouterPtr router;

    ASSERT_TRUE(impl.getLeaderRouter("zone", router));
    ASSERT_TRUE(router);
    ASSERT_EQ("zone", router->_zoneName);

    LeaderRouterPtr router2;
    ASSERT_TRUE(impl.getLeaderRouter("zone", router2));
    ASSERT_EQ(router.get(), router2.get());
}

} // namespace suez
