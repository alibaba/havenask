#include "suez/deploy/FileDeployer.h"

#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "suez/sdk/PathDefine.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace fslib::fs;

namespace suez {

class FileDeployerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    string _localPath;
};

void FileDeployerTest::setUp() {
    _localPath = GET_TEST_DATA_PATH() + "testFileDeployer_temp_dir";
    FileSystem::remove(_localPath);
}

void FileDeployerTest::tearDown() { FileSystem::remove(_localPath); }

TEST_F(FileDeployerTest, testDeployDoneFile) {
    string originalPath = "originalPath";
    string configPath = "config";
    auto doneFile = PathDefine::join(_localPath, FileDeployer::MARK_FILE_NAME);
    EXPECT_TRUE(FileDeployer::markDeployDone(doneFile, originalPath));
    EXPECT_EQ(fslib::EC_TRUE, fslib::fs::FileSystem::isFile(doneFile));
    EXPECT_TRUE(FileDeployer::checkDeployDone(doneFile, originalPath));

    string content;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(doneFile, content));
    EXPECT_EQ(originalPath, content);

    // test mark will overwrite duplicate doneFile
    EXPECT_TRUE(FileDeployer::markDeployDone(doneFile, originalPath + ".modified"));

    content.clear();
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(doneFile, content));
    EXPECT_EQ(originalPath + ".modified", content);
}

TEST_F(FileDeployerTest, testCompatible) {
    string remotePath = "dfs://remotePath";
    string rawPath = "";
    string rawPath2 = "rawPath2";
    string configPath = "";
    auto doneFile = PathDefine::join(_localPath, FileDeployer::MARK_FILE_NAME);

    // done file Version1 not compatible
    {
        string version1Content = rawPath;
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(doneFile, version1Content));
        ASSERT_FALSE(FileDeployer::checkDeployDone(doneFile, remotePath));
        ASSERT_TRUE(fslib::util::FileUtil::remove(doneFile));
    }

    // done file Version2 compatible
    {
        string version2Content = remotePath + ":" + rawPath;
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(doneFile, version2Content));
        ASSERT_TRUE(FileDeployer::checkDeployDone(doneFile, remotePath));
        ASSERT_TRUE(fslib::util::FileUtil::remove(doneFile));
    }
    {
        string version2Content = remotePath + ":" + rawPath2;
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(doneFile, version2Content));
        ASSERT_FALSE(FileDeployer::checkDeployDone(doneFile, remotePath));
        ASSERT_TRUE(fslib::util::FileUtil::remove(doneFile));
    }

    // done file Version3 compatible
    {
        string version3Content = remotePath + ":" + rawPath + ":" + configPath;
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(doneFile, version3Content));
        ASSERT_TRUE(FileDeployer::checkDeployDone(doneFile, remotePath));
        ASSERT_TRUE(fslib::util::FileUtil::remove(doneFile));
    }
}

} // namespace suez
