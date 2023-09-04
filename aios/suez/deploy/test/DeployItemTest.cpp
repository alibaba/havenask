#include "suez/deploy/DeployItem.h"

#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace fslib::fs;
using namespace testing;

namespace suez {

class DeployItemTest : public TESTBASE {
protected:
    void createFakeFiles(const string &path) {
        string content;
        content.resize(4 * 1024 * 1024);
#define MKDIR_WITHCHECK(subPath)                                                                                       \
    ASSERT_EQ(fslib::EC_OK, FileSystem::mkDir(FileSystem::joinFilePath(path, subPath), true))

#define WRITE_WITHCHECK(fileName)                                                                                      \
    ASSERT_EQ(fslib::EC_OK, FileSystem::writeFile(FileSystem::joinFilePath(path, fileName), content))
        MKDIR_WITHCHECK("");
        WRITE_WITHCHECK("log");
        WRITE_WITHCHECK("log3");
        MKDIR_WITHCHECK("config");
        WRITE_WITHCHECK("config/log");
        WRITE_WITHCHECK("config/log3");
        MKDIR_WITHCHECK("config2");
    }
    void createRemote() { createFakeFiles(_remote); }
    std::string _remote;
};

TEST_F(DeployItemTest, testGetDiskUse) {
    _remote = FileSystem::joinFilePath(GET_TEMPLATE_DATA_PATH(), "remote");
    createRemote();
    int64_t size;
    EXPECT_TRUE(DeployItem::getFilesSize(_remote, {"log", "config/log"}, size));
    EXPECT_EQ(8 * 1024 * 1024, size);
    EXPECT_TRUE(DeployItem::getFilesSize(_remote, {}, size));
    EXPECT_EQ(0, size);

    vector<string> files;
    EXPECT_TRUE(DeployItem::getFilesAndSize(_remote, files, size));
    EXPECT_THAT(files, ElementsAre("config", "config/log", "config/log3", "config2", "log", "log3"));
    EXPECT_EQ(16 * 1024 * 1024, size);

    files.clear();
    EXPECT_TRUE(DeployItem::getFilesAndSize(_remote + "/config2", files, size));
    EXPECT_THAT(files, ElementsAre());
    EXPECT_EQ(0, size);
}

} // namespace suez
