#include "suez/heartbeat/HeartbeatManager.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <iosfwd>
#include <map>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/SchedulerInfo.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace suez {

class HeartbeatManagerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    HeartbeatManager *_hbManager;
};

void HeartbeatManagerTest::setUp() { _hbManager = new HeartbeatManager; }

void HeartbeatManagerTest::tearDown() { DELETE_AND_SET_NULL(_hbManager); }

TEST_F(HeartbeatManagerTest, testGetTarget) {
    HeartbeatTarget heartbeatTarget;
    HeartbeatTarget finalHeartbeatTarget;
    SchedulerInfo schedulerInfo0;
    ASSERT_FALSE(_hbManager->getTarget(heartbeatTarget, finalHeartbeatTarget, schedulerInfo0));
    map<string, string> target;
    target["a"] = "b";
    target["c"] = "d";
    string targetStr = FastToJsonString(target);
    string signature = "signature";
    SchedulerInfo schedulerInfo;
    schedulerInfo._cm2TopoInfo = "cm2topoinfo";
    _hbManager->_signature = signature;
    _hbManager->_targetSnapShot = targetStr;
    _hbManager->_finalTargetSnapShot = "{}";
    _hbManager->_schedulerInfoStr = FastToJsonString(schedulerInfo);
    HeartbeatTarget heartbeatTarget1;
    HeartbeatTarget finalHeartbeatTarget1;
    SchedulerInfo schedulerInfo1;
    ASSERT_TRUE(_hbManager->getTarget(heartbeatTarget1, finalHeartbeatTarget1, schedulerInfo1));
    ASSERT_EQ(targetStr, FastToJsonString(heartbeatTarget1.getTarget()));
    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual("{\n}", FastToJsonString(finalHeartbeatTarget1.getTarget())));
    ASSERT_EQ(signature, heartbeatTarget1._signature);
    ASSERT_EQ(schedulerInfo._cm2TopoInfo, schedulerInfo1._cm2TopoInfo);

    _hbManager->_finalTargetSnapShot = "invalid json";
    HeartbeatTarget heartbeatTarget2;
    HeartbeatTarget finalHeartbeatTarget2;
    SchedulerInfo schedulerInfo2;
    ASSERT_TRUE(_hbManager->getTarget(heartbeatTarget2, finalHeartbeatTarget2, schedulerInfo2));
    ASSERT_EQ(targetStr, FastToJsonString(heartbeatTarget2.getTarget()));
    ASSERT_EQ(targetStr, FastToJsonString(finalHeartbeatTarget2.getTarget()));
    ASSERT_EQ(signature, heartbeatTarget1._signature);
    ASSERT_EQ(schedulerInfo._cm2TopoInfo, schedulerInfo2._cm2TopoInfo);
}

TEST_F(HeartbeatManagerTest, testSetCurrent) {
    JsonMap hb;
    hb["table_info"] = JsonMap();
    JsonMap si;
    si["cm2"] = JsonMap();
    string sig = "sig";
    _hbManager->setCurrent(hb, si, sig);
    ASSERT_EQ(FastToJsonString(hb), _hbManager->_currentSnapShot);
    ASSERT_EQ(FastToJsonString(si), _hbManager->_currentServiceInfo);
    ASSERT_EQ(sig, _hbManager->_currentSignature);
}

TEST_F(HeartbeatManagerTest, testGetGlobalCustomInfo) {
    string globalCustomInfo;
    EXPECT_FALSE(_hbManager->getGlobalCustomInfo(globalCustomInfo));
    EXPECT_TRUE(_hbManager->_finalTargetSnapShot == "");

    globalCustomInfo = "{";
    EXPECT_FALSE(_hbManager->getGlobalCustomInfo(globalCustomInfo));
    EXPECT_TRUE(_hbManager->_finalTargetSnapShot == "");

    globalCustomInfo = "{}";
    EXPECT_FALSE(_hbManager->getGlobalCustomInfo(globalCustomInfo));
    EXPECT_TRUE(_hbManager->_finalTargetSnapShot == "");

    JsonMap jsonMap;
    jsonMap["customInfo"] = JsonMap();
    globalCustomInfo = FastToJsonString(jsonMap);
    EXPECT_FALSE(_hbManager->getGlobalCustomInfo(globalCustomInfo));
    EXPECT_TRUE(_hbManager->_finalTargetSnapShot == "");

    jsonMap["customInfo"] = Any(string("testCustomInfo"));
    globalCustomInfo = FastToJsonString(jsonMap);
    EXPECT_TRUE(_hbManager->getGlobalCustomInfo(globalCustomInfo));
    EXPECT_TRUE(_hbManager->_finalTargetSnapShot == "testCustomInfo");
}

TEST_F(HeartbeatManagerTest, testProcessVersionMatch) {
    EXPECT_TRUE(_hbManager->processVersionMatch(""));
    EXPECT_TRUE(_hbManager->processVersionMatch("abc"));
    EXPECT_TRUE(_hbManager->processVersionMatch("abc:123"));

    _hbManager->_packageCheckSum = "abc";
    _hbManager->_procInstanceId = "123";
    EXPECT_FALSE(_hbManager->processVersionMatch(""));
    EXPECT_FALSE(_hbManager->processVersionMatch("abc:"));
    EXPECT_FALSE(_hbManager->processVersionMatch(":123"));
    EXPECT_FALSE(_hbManager->processVersionMatch(":123:321"));
    EXPECT_TRUE(_hbManager->processVersionMatch("abc:123#321"));
}

} // namespace suez
