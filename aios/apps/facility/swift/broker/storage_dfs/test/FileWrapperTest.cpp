#include "swift/broker/storage_dfs/FileWrapper.h"

#include <iostream>
#include <stdint.h>
#include <string>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
namespace swift {
namespace storage {

class FileWrapperTest : public TESTBASE {
public:
    void setUp() {
        fslib::ErrorCode ec = fslib::fs::FileSystem::remove(GET_TEMPLATE_DATA_PATH());
        ASSERT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
        ec = fslib::fs::FileSystem::mkDir(GET_TEMPLATE_DATA_PATH(), true);
        ASSERT_TRUE(ec == fslib::EC_OK);
    }
    bool prepartData(const string &fileName) {
        string metaStr;
        for (int64_t i = 0; i < 10; i++) {
            metaStr.append(1, 'a');
        }
        fslib::ErrorCode ec = fslib::fs::FileSystem::writeFile(fileName, metaStr);
        if (ec != fslib::EC_OK) {
            return false;
        }
        ec = fslib::fs::FileSystem::isExist(fileName);
        if (ec != fslib::EC_TRUE) {
            return false;
        }
        return true;
    }
};

TEST_F(FileWrapperTest, testGetFileLength) {
    string fileName = GET_TEMPLATE_DATA_PATH() + "/meta_1";
    ASSERT_TRUE(prepartData(fileName));
    FileWrapper fileWrapper(fileName);
    ASSERT_EQ(10, fileWrapper.getFileLength());
    ASSERT_TRUE(fileWrapper.open());
    ASSERT_EQ(10, fileWrapper.getFileLength());
    ASSERT_FALSE(fileWrapper.isAppending());

    FileWrapper fileWrapper1(fileName, true);
    ASSERT_EQ(10, fileWrapper1.getFileLength());
    ASSERT_TRUE(fileWrapper1.open());
    ASSERT_EQ(10, fileWrapper1.getFileLength());
    ASSERT_TRUE(fileWrapper1.isAppending());
}

TEST_F(FileWrapperTest, testPread) {
    string fileName = GET_TEMPLATE_DATA_PATH() + "/meta_1";
    ASSERT_TRUE(prepartData(fileName));
    FileWrapper fileWrapper(fileName);
    char buf[10];
    bool succ = false;
    int64_t readLen = fileWrapper.pread(buf, 2, 0, succ);
    ASSERT_TRUE(succ);
    ASSERT_EQ(2, readLen);
    ASSERT_EQ('a', buf[0]);
    readLen = fileWrapper.pread(buf, 5, 5, succ);
    ASSERT_EQ(10, fileWrapper.getFileLength());
    ASSERT_EQ(5, readLen);
    ASSERT_TRUE(succ);
    readLen = fileWrapper.pread(buf, 6, 5, succ);
    ASSERT_TRUE(succ);
    ASSERT_EQ(5, readLen);
    readLen = fileWrapper.pread(buf, 1, 9, succ);
    ASSERT_TRUE(succ);
    ASSERT_EQ(1, readLen);
    readLen = fileWrapper.pread(buf, 1, 10, succ);
    ASSERT_TRUE(succ);
    ASSERT_EQ(0, readLen);
    readLen = fileWrapper.pread(buf, 1, 11, succ);
    ASSERT_TRUE(succ);
    ASSERT_EQ(0, readLen);
}

} // namespace storage
} // namespace swift
