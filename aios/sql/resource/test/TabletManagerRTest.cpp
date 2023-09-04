#include "sql/resource/TabletManagerR.h"

#include <iosfwd>
#include <map>

#include "autil/Log.h"
#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "sql/resource/watermark/TabletWaiterInitOption.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace sql {

class TabletManagerRTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(resource, TabletManagerRTest);

void TabletManagerRTest::setUp() {}

void TabletManagerRTest::tearDown() {}

TEST_F(TabletManagerRTest, testCanSupportWaterMark) {
    TabletManagerR tabletManager;
    {
        // test realtime info not valid
        TabletWaiterInitOption option;
        ASSERT_FALSE(tabletManager.canSupportWaterMark(option));
    }
    {
        // test realtime info no realtime mode
        build_service::KeyValueMap kvMap;
        build_service::workflow::RealtimeInfoWrapper realtimeInfo(kvMap);
        TabletWaiterInitOption option;
        option.realtimeInfo = realtimeInfo;
        ASSERT_FALSE(tabletManager.canSupportWaterMark(option));
    }
    {
        // test realtime info realtime mode = realtime  service mode
        build_service::KeyValueMap kvMap;
        kvMap[build_service::config::REALTIME_MODE] = build_service::config::REALTIME_SERVICE_MODE;
        build_service::workflow::RealtimeInfoWrapper realtimeInfo(kvMap);
        TabletWaiterInitOption option;
        option.realtimeInfo = realtimeInfo;
        ASSERT_FALSE(tabletManager.canSupportWaterMark(option));
    }
    {
        // test realtime info realtime mode = realtime  job mode
        build_service::KeyValueMap kvMap;
        kvMap[build_service::config::REALTIME_MODE] = build_service::config::REALTIME_JOB_MODE;
        build_service::workflow::RealtimeInfoWrapper realtimeInfo(kvMap);
        TabletWaiterInitOption option;
        option.realtimeInfo = realtimeInfo;
        ASSERT_TRUE(tabletManager.canSupportWaterMark(option));
    }
    {
        // test realtime info realtime mode = realtime  raw doc rt build mode
        build_service::KeyValueMap kvMap;
        kvMap[build_service::config::REALTIME_MODE]
            = build_service::config::REALTIME_SERVICE_RAWDOC_RT_BUILD_MODE;
        build_service::workflow::RealtimeInfoWrapper realtimeInfo(kvMap);
        TabletWaiterInitOption option;
        option.realtimeInfo = realtimeInfo;
        ASSERT_TRUE(tabletManager.canSupportWaterMark(option));
    }
    {
        // test realtime info realtime mode = other
        build_service::KeyValueMap kvMap;
        kvMap[build_service::config::REALTIME_MODE] = "other";
        build_service::workflow::RealtimeInfoWrapper realtimeInfo(kvMap);
        TabletWaiterInitOption option;
        option.realtimeInfo = realtimeInfo;
        ASSERT_FALSE(tabletManager.canSupportWaterMark(option));
    }
}

} // namespace sql
