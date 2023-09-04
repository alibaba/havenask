#ifndef WORKER_FRAMEWORK_LEADERINFOTEST_H
#define WORKER_FRAMEWORK_LEADERINFOTEST_H

#include "worker_framework/LeaderInfo.h"

#include "autil/Log.h"
#include "test/test.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
namespace worker_framework {

class LeaderInfoTest : public testing::Test {
protected:
    LeaderInfoTest() {}

    virtual ~LeaderInfoTest() {}

    virtual void SetUp() {}

    virtual void TearDown() {}

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(worker_framework.worker_base, LeaderInfoTest);

TEST_F(LeaderInfoTest, testLeaderInfo) {
    LeaderInfo info;
    EXPECT_EQ("{\n\n}\n", info.toString());

    info.set("abc", 123);
    info.set("cdb", 123.2);
    info.setHttpAddress("123.2.2.2", 1234);
    EXPECT_EQ(
        "{\n\t\"abc\" : 123,\n\t\"cdb\" : 123.2,\n\t\"httpAddress\" : \"123.2.2.2:1234\",\n\t\"httpPort\" : 1234\n}\n",
        info.toString());
    /*
    {
        "abc" : 123,
        "cdb" : 123.2,
        "httpAddress" : "123.2.2.2:1234",
        "httpPort" : 1234
    }

    */
}

}; // namespace worker_framework

#endif // WORKER_FRAMEWORK_LEADERINFOTEST_H
