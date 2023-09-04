#include "suez/table/SimpleVersionPublisher.h"

#include "fslib/fs/FileSystem.h"
#include "suez/common/TableMeta.h"
#include "suez/common/TablePathDefine.h"
#include "suez/common/test/TableMetaUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace suez {

class SimpleVersionPublisherTest : public TESTBASE {};

TEST_F(SimpleVersionPublisherTest, testSimple) {
    auto pid = TableMetaUtil::makePid("table1", 1, 0, 32767);
    auto indexRoot = GET_TEMP_DATA_PATH();
    auto partitionRoot = TablePathDefine::constructIndexPath(indexRoot, pid);
    auto ec = fslib::fs::FileSystem::mkDir(partitionRoot, true);
    ASSERT_EQ(fslib::EC_OK, ec);

    SimpleVersionPublisher publisher;
    TableVersion published;
    ASSERT_FALSE(publisher.getPublishedVersion(pid, published));
    TableVersion version(0, indexlibv2::framework::VersionMeta(), false);
    ASSERT_TRUE(publisher.publish(indexRoot, pid, version));
    ASSERT_TRUE(publisher.getPublishedVersion(pid, published));
    ASSERT_TRUE(published == version);
    TableVersion published2;
    ASSERT_FALSE(publisher.getPublishedVersion(indexRoot, pid, 1, published2));
    ASSERT_TRUE(publisher.getPublishedVersion(indexRoot, pid, 0, published2));
    ASSERT_EQ(version, published2);

    string content;
    string versionPath = fslib::fs::FileSystem::joinFilePath(partitionRoot, "version.publish.0");
    ec = fslib::fs::FileSystem::isExist(versionPath);
    ASSERT_EQ(fslib::EC_TRUE, ec);
    ec = fslib::fs::FileSystem::readFile(versionPath, content);
    ASSERT_EQ(fslib::EC_OK, ec);
    ASSERT_EQ(FastToJsonString(version, true), content);

    TableVersion version2(1, indexlibv2::framework::VersionMeta(), true);
    ASSERT_TRUE(publisher.publish(indexRoot, pid, version2));
    ASSERT_TRUE(publisher.getPublishedVersion(pid, published));
    ASSERT_TRUE(published == version2);

    TableVersion version3(1, indexlibv2::framework::VersionMeta(), true);
    ASSERT_FALSE(publisher.publish(indexRoot, pid, version3));
    ASSERT_TRUE(publisher.getPublishedVersion(pid, published));
    ASSERT_TRUE(published == version2);

    publisher.remove(pid);
    ASSERT_FALSE(publisher.getPublishedVersion(pid, published));
}

} // namespace suez
