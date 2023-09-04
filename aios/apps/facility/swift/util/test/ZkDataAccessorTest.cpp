#include "swift/util/ZkDataAccessor.h"

#include <iostream>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace swift::protocol;
using namespace fslib;
using namespace fslib::fs;
using namespace std;
using namespace autil;

namespace swift {
namespace util {

class ZkDataAccessorTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

protected:
    cm_basic::ZkWrapper *_zkWrapper = nullptr;
    zookeeper::ZkServer *_zkServer = nullptr;
    string _zkRoot;
};

void ZkDataAccessorTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    std::ostringstream oss;
    oss << "127.0.0.1:" << _zkServer->port();
    _zkRoot = "zfs://" + oss.str() + "/";
    _zkWrapper = new cm_basic::ZkWrapper(oss.str(), 1000);
    ASSERT_TRUE(_zkWrapper->open());
    cout << "zkRoot: " << _zkRoot << endl;
}

void ZkDataAccessorTest::tearDown() { DELETE_AND_SET_NULL(_zkWrapper); }

TEST_F(ZkDataAccessorTest, testInitByOwnZkWrapper) {
    ZkDataAccessor da;
    ASSERT_TRUE(da.init(_zkRoot));
}

TEST_F(ZkDataAccessorTest, testSimple) {
    ZkDataAccessor da;
    EXPECT_TRUE(da.init(_zkWrapper, _zkRoot));
    protocol::WriterVersion writeVersion;
    writeVersion.set_name("test_writer");
    writeVersion.set_version(100);

    auto versionFile = FileSystem::joinFilePath(_zkRoot, "version");
    auto versionPath = "/version";
    EXPECT_EQ(EC_FALSE, FileSystem::isExist(versionFile));
    EXPECT_TRUE(da.createPath(versionPath));
    EXPECT_TRUE(da.writeZk(versionPath, writeVersion, false, false));

    // 1. normal version
    protocol::WriterVersion readVersion;
    EXPECT_TRUE(da.readZk(versionPath, readVersion, false, false));
    EXPECT_EQ(writeVersion.name(), readVersion.name());
    EXPECT_EQ(writeVersion.version(), readVersion.version());
    EXPECT_TRUE(da.remove(versionPath));
    EXPECT_EQ(EC_FALSE, FileSystem::isExist(versionFile));

    // 2. compress version
    protocol::WriterVersion compressVersion;
    EXPECT_TRUE(da.writeZk(versionPath, writeVersion, true, false));
    EXPECT_TRUE(da.readZk(versionPath, compressVersion, true, false));
    EXPECT_EQ(writeVersion.name(), compressVersion.name());
    EXPECT_EQ(writeVersion.version(), compressVersion.version());
    EXPECT_TRUE(da.remove(versionPath));
    EXPECT_EQ(EC_FALSE, FileSystem::isExist(versionFile));

    // 3. json version
    protocol::WriterVersion jsonVersion;
    EXPECT_TRUE(da.writeZk(versionPath, writeVersion, false, true));
    EXPECT_TRUE(da.readZk(versionPath, jsonVersion, false, true));
    EXPECT_EQ(writeVersion.name(), jsonVersion.name());
    EXPECT_EQ(writeVersion.version(), jsonVersion.version());
    EXPECT_TRUE(da.remove(versionPath));
    EXPECT_EQ(EC_FALSE, FileSystem::isExist(versionFile));

    // 4. json compress version
    protocol::WriterVersion jsonCompressVersion;
    EXPECT_TRUE(da.writeZk(versionPath, writeVersion, true, true));
    EXPECT_TRUE(da.readZk(versionPath, jsonCompressVersion, true, true));
    EXPECT_EQ(writeVersion.name(), jsonCompressVersion.name());
    EXPECT_EQ(writeVersion.version(), jsonCompressVersion.version());
}

} // namespace util
} // namespace swift
