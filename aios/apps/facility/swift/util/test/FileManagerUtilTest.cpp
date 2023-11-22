#include "swift/util/FileManagerUtil.h"

#include <functional>
#include <iosfwd>
#include <stdint.h>
#include <stdlib.h>
#include <string>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/FileSystemFactory.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/common/FilePair.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;
using namespace swift::common;
using namespace swift::storage;

namespace swift {
namespace util {
class FileManagerUtilTest : public TESTBASE {
private:
    void touchFile(const std::string &fileName, uint32_t count);
    FilePairPtr createFilePair(std::string metaFileName,
                               std::string dataFileName,
                               int64_t msgId,
                               int64_t count,
                               int64_t timeStamp,
                               bool needTouchFile = true);
};

void FileManagerUtilTest::touchFile(const string &fileName, uint32_t count) {
    FilePtr file(FileSystem::openFile(fileName, fslib::WRITE));
    string str(count * FILE_MESSAGE_META_SIZE, 'a');
    file->write(str.data(), str.length());
    file->close();
}

FilePairPtr FileManagerUtilTest::createFilePair(
    string metaFileName, string dataFileName, int64_t msgId, int64_t count, int64_t timeStamp, bool needTouchFile) {
    FilePairPtr filePairPtr(new FilePair(dataFileName, metaFileName, msgId, timeStamp));
    if (needTouchFile) {
        touchFile(filePairPtr->metaFileName, count);
        touchFile(filePairPtr->dataFileName, count);
    }
    //    filePairPtr->endMessageId = msgId + count;
    return filePairPtr;
}

TEST_F(FileManagerUtilTest, testRemoveFilePairWithSize) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testFileManagerUtil/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);

    // remove dataFile and metaFile
    int64_t currTime = TimeUtility::currentTime();
    FilePairPtr filePairPtr = createFilePair(dataRoot + "metafile1", dataRoot + "datafile1", 100, 100, currTime);
    int64_t fileSize;
    EXPECT_TRUE(FileManagerUtil::removeFilePairWithSize(filePairPtr, fileSize));
    EXPECT_EQ(200 * FILE_MESSAGE_META_SIZE, fileSize);

    // remove dataFile only
    filePairPtr = createFilePair(dataRoot + "metafile2", dataRoot + "datafile2", 100, 100, currTime);
    FileSystem::remove(dataRoot + "metafile2");
    EXPECT_TRUE(FileManagerUtil::removeFilePairWithSize(filePairPtr, fileSize));
    EXPECT_EQ(100 * FILE_MESSAGE_META_SIZE, fileSize);

    // remove metaFile only
    filePairPtr = createFilePair(dataRoot + "metafile3", dataRoot + "datafile3", 100, 100, currTime);
    FileSystem::remove(dataRoot + "datafile3");
    EXPECT_TRUE(FileManagerUtil::removeFilePairWithSize(filePairPtr, fileSize));
    EXPECT_EQ(100 * FILE_MESSAGE_META_SIZE, fileSize);

    // remove not exsit file
    filePairPtr = createFilePair(dataRoot + "metafile4", dataRoot + "datafile4", 100, 100, currTime);
    FileSystem::remove(dataRoot + "metafile4");
    FileSystem::remove(dataRoot + "datafile4");
    EXPECT_TRUE(FileManagerUtil::removeFilePairWithSize(filePairPtr, fileSize));
    EXPECT_EQ(0, fileSize);

    // remove error
    filePairPtr = createFilePair(string(300, 'a'), dataRoot + "datafile5", 100, 100, currTime);
    EXPECT_FALSE(FileManagerUtil::removeFilePairWithSize(filePairPtr, fileSize));
    EXPECT_EQ(100 * FILE_MESSAGE_META_SIZE, fileSize);

    filePairPtr = createFilePair(dataRoot + "metafile5", string(300, 'a'), 100, 100, currTime);
    EXPECT_FALSE(FileManagerUtil::removeFilePairWithSize(filePairPtr, fileSize));
    EXPECT_EQ(100 * FILE_MESSAGE_META_SIZE, fileSize);

    FileSystem::remove(dataRoot);
}

TEST_F(FileManagerUtilTest, testGetFileSize) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testFileManagerUtil/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);

    touchFile(dataRoot + "metafile1", 100);
    int64_t fileSize = FileManagerUtil::getFileSize(dataRoot + "metafile1");
    EXPECT_EQ(100 * FILE_MESSAGE_META_SIZE, fileSize);

    EXPECT_TRUE(FileManagerUtil::removeFile(dataRoot + "metafile1"));
    fileSize = FileManagerUtil::getFileSize(dataRoot + "metafile1");
    EXPECT_EQ(0, fileSize);

    FileSystem::remove(dataRoot);
}

TEST_F(FileManagerUtilTest, testRemoveFile) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testFileManagerUtil/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);

    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(dataRoot + "metafile1"));
    EXPECT_TRUE(FileManagerUtil::removeFile(dataRoot + "metafile1"));

    touchFile(dataRoot + "metafile1", 100);
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(dataRoot + "metafile1"));
    EXPECT_TRUE(FileManagerUtil::removeFile(dataRoot + "metafile1"));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(dataRoot + "metafile1"));

    // filename too long, remove error
    EXPECT_FALSE(FileManagerUtil::removeFile(string(300, 'a')));

    FileSystem::remove(dataRoot);
}
} // namespace util
} // namespace swift
