#include "swift/broker/service/BrokerHealthChecker.h"

#include <iosfwd>
#include <string>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace fslib::fs;
using namespace fslib::util;

namespace swift {
namespace service {

class BrokerHealthCheckerTest : public TESTBASE {
public:
    BrokerHealthCheckerTest() {}
    ~BrokerHealthCheckerTest() {}

public:
    void setUp() {}
    void tearDown() {}
};

TEST_F(BrokerHealthCheckerTest, testInit) {
    BrokerHealthChecker brokerHealthChecker;
    string dfsPath = "dfs://1.2.3.4/topic";
    string zkPath = "zfs://5.6.7.8/topic";
    string roleName = "badRoleName";
    EXPECT_FALSE(brokerHealthChecker.init(dfsPath, zkPath, roleName));
    roleName = "default##_0_";
    EXPECT_FALSE(brokerHealthChecker.init(dfsPath, zkPath, roleName));
    roleName = "default##_1_2";
    EXPECT_TRUE(brokerHealthChecker.init(dfsPath, zkPath, roleName));
    string expectDfsPath = "dfs://1.2.3.4/topic/_status_/2/default##_1";
    string expectZkPath = "zfs://5.6.7.8/topic/_status_/2/default##_1";
    string expectLocalPath = "./_status_/2/default##_1";
    EXPECT_EQ(expectDfsPath, brokerHealthChecker._dfsPath);
    EXPECT_EQ(expectZkPath, brokerHealthChecker._zkPath);
    EXPECT_EQ(expectLocalPath, brokerHealthChecker._localPath);
}

TEST_F(BrokerHealthCheckerTest, testCheckWrite) {
    string basePath = GET_TEST_DATA_PATH();
    const string &dfsFileName = "mock://" + basePath + "/dfs/fileName";
    BrokerHealthChecker brokerHealthChecker;
    string content = "check status 1";
    EXPECT_TRUE(brokerHealthChecker.checkWrite("", content));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(dfsFileName));

    content = "check status 2";
    FileSystem::_useMock = true;
    ExceptionTrigger::InitTrigger(0, true, false);

    ExceptionTrigger::SetExceptionCondition("openFile", dfsFileName, fslib::EC_NOTSUP);
    EXPECT_FALSE(brokerHealthChecker.checkWrite(dfsFileName, content));

    content = "check status 3";
    FileSystem::_useMock = false;
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(dfsFileName));
    EXPECT_TRUE(brokerHealthChecker.checkWrite(dfsFileName, content));
    string fileContent;
    FileSystem::readFile(dfsFileName, fileContent);
    EXPECT_EQ(content, fileContent);
}
} // namespace service
} // namespace swift
