#include "swift/broker/storage_dfs/FileManager.h"

#include <algorithm>
#include <bits/types/struct_tm.h>
#include <functional>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <iostream>
#include <memory>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/DllWrapper.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/FileSystemFactory.h"
#include "swift/broker/storage_dfs/test/MockFileManager.h"
#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/common/FilePair.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/FileManagerUtil.h"
#include "swift/util/PanguInlineFileUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;
using namespace swift::common;
using namespace swift::protocol;
using namespace swift::util;
using namespace testing;

using ::testing::Ge;
using ::testing::Le;

namespace swift {
namespace storage {

#define FSLIB_MOCK_FORWARD_FUNCTION_NAME "mockForward"
class FileManagerTest : public TESTBASE {
private:
    void singleFileNameConvert(int64_t msgId, int64_t timestamp);
    bool prepareRecover(const std::string &root);
    void touchFile(const std::string &fileName, uint32_t count);
    FilePairPtr createFilePair(std::string metaFileName,
                               std::string dataFileName,
                               int64_t msgId,
                               int64_t count,
                               int64_t timeStamp,
                               bool needTouchFile = true);
    bool extractMessageIdAndTimestampOld(const string &prefix, int64_t &msgId, int64_t &timestamp);
};

TEST_F(FileManagerTest, testGetFilePairById) {
    { // empty
        FileManager manager;
        FilePairPtr filePair = manager.getFilePairById(0);
        EXPECT_TRUE(filePair == NULL);
    }
    { // one file
        FileManager manager;
        int64_t curTime = TimeUtility::currentTime();
        FilePairPtr pairPtr = manager.createNewFilePair(0, curTime);
        EXPECT_TRUE(pairPtr);
        FilePairPtr pair1 = manager.getFilePairById(0);
        EXPECT_TRUE(NULL == pair1);
        pairPtr->setEndMessageId(1);
        pair1 = manager.getFilePairById(0);
        EXPECT_EQ(pairPtr, pair1);
    }
    { // one file
        string dataRoot = GET_TEMPLATE_DATA_PATH() + "testGetFilePairById/";
        FileSystem::remove(dataRoot);
        FileSystem::mkDir(dataRoot);
        string meta1 = dataRoot + "m1";
        touchFile(meta1, 3);
        FileManager manager;
        FilePairPtr f1(new FilePair("d", meta1, 2, 0));
        manager._filePairVec.push_back(f1);
        manager.adjustFilePairVec(manager._filePairVec);
        FilePairPtr pair1 = manager.getFilePairById(0);
        EXPECT_EQ(f1, pair1);
        pair1 = manager.getFilePairById(2);
        EXPECT_EQ(f1, pair1);
        pair1 = manager.getFilePairById(3);
        EXPECT_EQ(f1, pair1);
        pair1 = manager.getFilePairById(4);
        EXPECT_EQ(f1, pair1);
        pair1 = manager.getFilePairById(5);
        EXPECT_TRUE(NULL == pair1);
        FileSystem::remove(dataRoot);
    }
    { // multi file whit has same beginid
        string dataRoot = GET_TEMPLATE_DATA_PATH() + "testGetFilePairById/";
        FileSystem::remove(dataRoot);
        FileSystem::mkDir(dataRoot);
        string meta1 = dataRoot + "m1";
        string meta2 = dataRoot + "m2";
        string meta3 = dataRoot + "m3";
        string meta4 = dataRoot + "m4";
        string meta5 = dataRoot + "m5";
        string meta6 = dataRoot + "m6";
        string meta7 = dataRoot + "m7";
        string meta8 = dataRoot + "m8";
        string meta9 = dataRoot + "m9";
        touchFile(meta1, 1);
        touchFile(meta2, 1);
        touchFile(meta3, 1);
        touchFile(meta4, 1);
        touchFile(meta5, 1);
        touchFile(meta6, 1);
        touchFile(meta7, 1);
        touchFile(meta8, 1);
        touchFile(meta9, 1);
        FileManager manager;
        FilePairPtr f1(new FilePair("d", meta1, 0, 0));
        FilePairPtr f2(new FilePair("d", meta2, 0, 1));
        FilePairPtr f3(new FilePair("d", meta1, 1, 2));
        FilePairPtr f4(new FilePair("d", meta2, 1, 3));
        FilePairPtr f5(new FilePair("d", meta1, 1, 4));
        FilePairPtr f6(new FilePair("d", meta2, 2, 5));
        FilePairPtr f7(new FilePair("d", meta1, 3, 6));
        FilePairPtr f8(new FilePair("d", meta2, 5, 7));
        FilePairPtr f9(new FilePair("d", meta2, 5, 8));
        manager._filePairVec.push_back(f1);
        manager._filePairVec.push_back(f2);
        manager._filePairVec.push_back(f3);
        manager._filePairVec.push_back(f4);
        manager._filePairVec.push_back(f5);
        manager._filePairVec.push_back(f6);
        manager._filePairVec.push_back(f7);
        manager._filePairVec.push_back(f8);
        manager._filePairVec.push_back(f9);
        manager.adjustFilePairVec(manager._filePairVec);
        FilePairPtr pair1 = manager.getFilePairById(0);
        EXPECT_EQ(f2, pair1);
        pair1 = manager.getFilePairById(1);
        EXPECT_EQ(f5, pair1);
        pair1 = manager.getFilePairById(2);
        EXPECT_EQ(f6, pair1);
        pair1 = manager.getFilePairById(3);
        EXPECT_EQ(f7, pair1);
        pair1 = manager.getFilePairById(4);
        EXPECT_EQ(f7, pair1);
        pair1 = manager.getFilePairById(5);
        EXPECT_EQ(f9, pair1);
        pair1 = manager.getFilePairById(6);
        EXPECT_TRUE(NULL == pair1);
        FileSystem::remove(dataRoot);
    }
}

TEST_F(FileManagerTest, testInitFromEmpty) {
    FileManager *manager = new FileManager();
    unique_ptr<FileManager> ptr(manager);
    string root = GET_TEMPLATE_DATA_PATH() + "empty";
    fslib::fs::FileSystem::remove(root);
    ObsoleteFileCriterion obsoleteFileCriterion;
    protocol::ErrorCode ec = manager->init(root, obsoleteFileCriterion);
    EXPECT_EQ(protocol::ERROR_NONE, ec);
    fslib::ErrorCode fsEC = fslib::fs::FileSystem::isExist(root);
    EXPECT_EQ(fslib::EC_TRUE, fsEC);
    EXPECT_EQ((int64_t)-1, manager->getLastMessageId());
    EXPECT_EQ((int64_t)-1, manager->getMinMessageId());

    FilePairPtr pairPtr = manager->getLastFilePair();
    EXPECT_EQ(FilePairPtr(), pairPtr);
    pairPtr = manager->getFilePairByTime(0);
    EXPECT_EQ(FilePairPtr(), pairPtr);
    pairPtr = manager->getFilePairById(0);
    EXPECT_EQ(FilePairPtr(), pairPtr);

    // create a new file pair
    int64_t curTime = TimeUtility::currentTime();
    pairPtr = manager->createNewFilePair(0, curTime);
    EXPECT_TRUE(pairPtr->isAppending());
    EXPECT_TRUE(pairPtr);
    FilePairPtr pair1 = manager->getLastFilePair();
    EXPECT_EQ(pairPtr, pair1);
    EXPECT_TRUE(pair1->isAppending());

    pair1 = manager->getFilePairById(0);
    EXPECT_TRUE(NULL == pair1);
    pair1 = manager->getFilePairByTime(curTime);
    EXPECT_EQ(pairPtr, pair1);

    pair1 = manager->getFilePairByTime(curTime + 1);
    EXPECT_EQ(pairPtr, pair1);
    EXPECT_EQ((int64_t)-1, manager->getLastMessageId());
    EXPECT_EQ((int64_t)0, manager->getMinMessageId());

    pairPtr->setEndMessageId(1);
    pair1 = manager->getFilePairById(0);
    EXPECT_EQ(pairPtr, pair1);
}

TEST_F(FileManagerTest, testAdjustFilePairVec) {
    { // normal
        string dataRoot = GET_TEMPLATE_DATA_PATH() + "testAdjustFilePairVec/";
        FileSystem::remove(dataRoot);
        FileSystem::mkDir(dataRoot);
        string meta1 = dataRoot + "m1";
        touchFile(meta1, 2);
        FileManager manager;
        FilePairPtr f1(new FilePair("d", meta1, 0, 0));
        FilePairVec filePairVec;
        filePairVec.push_back(f1);
        ErrorCode ec = manager.adjustFilePairVec(filePairVec);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ((size_t)1, filePairVec.size());
        EXPECT_EQ(int64_t(2), filePairVec[0]->getEndMessageId());
        EXPECT_TRUE(filePairVec[0]->getNext() == NULL);
    }
    { // only one empty file
        string dataRoot = GET_TEMPLATE_DATA_PATH() + "testAdjustFilePairVec/";
        FileSystem::remove(dataRoot);
        FileSystem::mkDir(dataRoot);
        string meta1 = dataRoot + "m1";
        touchFile(meta1, 0);
        FileManager manager;
        FilePairPtr f1(new FilePair("d", meta1, 0, 0));
        FilePairVec filePairVec;
        filePairVec.push_back(f1);
        ErrorCode ec = manager.adjustFilePairVec(filePairVec);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ((size_t)0, filePairVec.size());
    }
    { // multi file with empty file
        string dataRoot = GET_TEMPLATE_DATA_PATH() + "testAdjustFilePairVec/";
        FileSystem::remove(dataRoot);
        FileSystem::mkDir(dataRoot);
        string meta1 = dataRoot + "m1";
        string meta2 = dataRoot + "m2";
        string meta3 = dataRoot + "m3";
        string meta4 = dataRoot + "m4";
        string meta5 = dataRoot + "m5";
        touchFile(meta1, 1);
        touchFile(meta2, 0);
        touchFile(meta3, 1);
        touchFile(meta4, 0);
        touchFile(meta5, 0);
        FileManager manager;
        FilePairPtr f1(new FilePair("d", meta1, 0, 0));
        FilePairPtr f2(new FilePair("d", meta2, 1, 0));
        FilePairPtr f3(new FilePair("d", meta3, 1, 0));
        FilePairPtr f4(new FilePair("d", meta4, 2, 0));
        FilePairPtr f5(new FilePair("d", meta5, 2, 0));

        FilePairVec filePairVec;
        filePairVec.push_back(f1);
        filePairVec.push_back(f2);
        filePairVec.push_back(f3);
        filePairVec.push_back(f4);
        filePairVec.push_back(f5);
        ErrorCode ec = manager.adjustFilePairVec(filePairVec);
        EXPECT_EQ(ERROR_NONE, ec);
        EXPECT_EQ((size_t)3, filePairVec.size());
        EXPECT_EQ((int64_t)1, filePairVec[0]->getEndMessageId());
        EXPECT_EQ((int64_t)1, filePairVec[1]->getEndMessageId());
        EXPECT_EQ((int64_t)2, filePairVec[2]->getEndMessageId());
        EXPECT_TRUE(filePairVec[0]->getNext() != NULL);
        EXPECT_TRUE(filePairVec[1]->getNext() != NULL);
        EXPECT_TRUE(filePairVec[2]->getNext() == NULL);
    }
    { // multi file with empty file and error file
        string dataRoot = GET_TEMPLATE_DATA_PATH() + "testAdjustFilePairVec/";
        FileSystem::remove(dataRoot);
        FileSystem::mkDir(dataRoot);
        string meta1 = dataRoot + "m1";
        string meta2 = dataRoot + "m2";
        string meta3 = dataRoot + "m3";
        string meta4 = dataRoot + "m4";
        string meta5 = dataRoot + "m5";
        touchFile(meta1, 1);
        touchFile(meta2, 0);
        touchFile(meta3, 1);
        touchFile(meta5, 0);
        FileManager manager;
        FilePairPtr f1(new FilePair("d", meta1, 0, 0));
        FilePairPtr f2(new FilePair("d", meta2, 1, 0));
        FilePairPtr f3(new FilePair("d", meta3, 1, 0));
        FilePairPtr f4(new FilePair("d", meta4, 2, 0));
        FilePairPtr f5(new FilePair("d", meta5, 2, 0));
        FilePairVec filePairVec;
        filePairVec.push_back(f1);
        filePairVec.push_back(f2);
        filePairVec.push_back(f3);
        filePairVec.push_back(f4);
        filePairVec.push_back(f5);
        ErrorCode ec = manager.adjustFilePairVec(filePairVec);
        EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
        EXPECT_EQ((size_t)4, filePairVec.size());
        EXPECT_EQ((int64_t)1, filePairVec[0]->getEndMessageId());
        EXPECT_EQ((int64_t)1, filePairVec[1]->getEndMessageId());
        EXPECT_EQ((int64_t)2, filePairVec[2]->getEndMessageId());
        int64_t msgId;
        EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, filePairVec[3]->getEndMessageId(msgId));
    }
}

TEST_F(FileManagerTest, testFileNameConvert) {
    int64_t timestamp = TimeUtility::currentTime();
    singleFileNameConvert(0, 0);
    singleFileNameConvert(1, 1);
    singleFileNameConvert(1, 0);
    singleFileNameConvert(0, 1);
    singleFileNameConvert(1234567890ll, 1234567890ll);
    singleFileNameConvert(12345678901234ll, timestamp);
}

void FileManagerTest::touchFile(const string &fileName, uint32_t count) {
    FilePtr file(FileSystem::openFile(fileName, fslib::WRITE));
    string str(count * FILE_MESSAGE_META_SIZE, 'a');
    file->write(str.data(), str.length());
    file->close();
}

void FileManagerTest::singleFileNameConvert(int64_t msgId, int64_t timestamp) {
    {
        string prefix = FileManagerUtil::generateFilePrefix(msgId, timestamp, false);
        int64_t id;
        int64_t time;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, id, time);
        EXPECT_TRUE(ret);
        EXPECT_EQ(msgId, id);
        EXPECT_EQ(timestamp, time);
    }
    {
        string prefix = FileManagerUtil::generateFilePrefix(msgId, timestamp, true);
        int64_t id;
        int64_t time;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, id, time);
        EXPECT_TRUE(ret);
        EXPECT_EQ(msgId, id);
        EXPECT_EQ(timestamp, time);
    }
}

bool FileManagerTest::prepareRecover(const std::string &root) {
    fslib::fs::FileSystem::remove(root);
    fslib::ErrorCode fsEC = fslib::fs::FileSystem::mkDir(root, true);
    EXPECT_EQ(fslib::EC_OK, fsEC);
    return true;
}

bool FileManagerTest::extractMessageIdAndTimestampOld(const string &prefix, int64_t &msgId, int64_t &timestamp) {
    int us = 0;
    struct tm xx;
    xx.tm_isdst = -1;
    int ret = sscanf(prefix.c_str(),
                     "%4d-%2d-%2d-%2d-%2d-%2d.%d.%ld",
                     &xx.tm_year,
                     &xx.tm_mon,
                     &xx.tm_mday,
                     &xx.tm_hour,
                     &xx.tm_min,
                     &xx.tm_sec,
                     &us,
                     &msgId);
    xx.tm_mon -= 1;
    xx.tm_year -= 1900;
    if (ret != 8)
        return false;
    time_t sec = mktime(&xx);
    timestamp = sec * 1000000ll + us;
    return true;
}

TEST_F(FileManagerTest, testGeneratePrefix) {
    int64_t msgId = 123;
    int64_t timestamp = 1582510280113497;
    {
        const string &prefix = FileManagerUtil::generateFilePrefix(msgId, timestamp, false);
        cout << prefix << endl;
        const vector<string> &prefixVec = StringUtil::split(prefix, ".");
        EXPECT_EQ(3, prefixVec.size());
        EXPECT_EQ("0000000000000123", prefixVec[2]);
    }
    {
        const string &prefix = FileManagerUtil::generateFilePrefix(msgId, timestamp, true);
        cout << prefix << endl;
        const vector<string> &prefixVec = StringUtil::split(prefix, ".");
        EXPECT_EQ(4, prefixVec.size());
        EXPECT_EQ("0000000000000123", prefixVec[2]);
        EXPECT_EQ("1582510280113497", prefixVec[3]);
    }
}

TEST_F(FileManagerTest, testExtractMessageIdAndTimestamp) {
    int64_t expectMsgId = 865596816535, expectTimestamp = 1582510280113497;
    { //老格式
        string prefix("2020-02-24-10-11-20.113497.0000865596816535");
        int64_t msgId = 0, timestamp = 0;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, msgId, timestamp);
        EXPECT_TRUE(ret);
        EXPECT_EQ(expectMsgId, msgId);
        EXPECT_EQ(expectTimestamp, timestamp);

        msgId = 0, timestamp = 0;
        ret = extractMessageIdAndTimestampOld(prefix, msgId, timestamp);
        EXPECT_TRUE(ret);
        EXPECT_EQ(expectMsgId, msgId);
        EXPECT_EQ(expectTimestamp, timestamp);
    }
    { //临时格式，后面要删除掉, 中间版本回退不支持，老版本不能兼容这种数据格式
        string prefix("2020-02-24-10-11-20.113497_1582510280113497.0000865596816535");
        int64_t msgId = 0, timestamp = 0;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, msgId, timestamp);
        EXPECT_TRUE(ret);
        EXPECT_EQ(expectMsgId, msgId);
        EXPECT_EQ(expectTimestamp, timestamp);

        msgId = 0, timestamp = 0;
        ret = extractMessageIdAndTimestampOld(prefix, msgId, timestamp);
        EXPECT_FALSE(ret); //不兼容，所以要去掉
    }
    { //新格式，新老版本兼容格式
        string prefix("2020-02-24-10-11-20.113497.0000865596816535.1582510280113497");
        int64_t msgId = 0, timestamp = 0;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, msgId, timestamp);
        EXPECT_TRUE(ret);
        EXPECT_EQ(expectMsgId, msgId);
        EXPECT_EQ(expectTimestamp, timestamp);

        msgId = 0, timestamp = 0;
        ret = extractMessageIdAndTimestampOld(prefix, msgId, timestamp);
        EXPECT_TRUE(ret);
        EXPECT_EQ(expectMsgId, msgId);
        EXPECT_EQ(expectTimestamp, timestamp);
    }
    { //新格式，文件日期时间与时间错不一致时，采用时间戳的时间为准
        string prefix("2020-02-24-10-11-20.113497.0000865596816535.1582510280000000");
        int64_t msgId = 0, timestamp = 0;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, msgId, timestamp);
        EXPECT_TRUE(ret);
        EXPECT_EQ(expectMsgId, msgId);
        EXPECT_EQ(1582510280000000, timestamp);

        msgId = 0, timestamp = 0;
        ret = extractMessageIdAndTimestampOld(prefix, msgId, timestamp);
        EXPECT_TRUE(ret);
        EXPECT_EQ(expectMsgId, msgId);
        EXPECT_EQ(expectTimestamp, timestamp);
    }
}

TEST_F(FileManagerTest, testExtractIllegeFile) {
    {
        string prefix = "1970-01-01-09-00-00.0000000000000000";
        int64_t id;
        int64_t time;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, id, time);
        EXPECT_FALSE(ret);
    }
    {
        string prefix = "1970-01-01-09-00-00.000050_0000000000000000";
        int64_t id;
        int64_t time;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, id, time);
        EXPECT_FALSE(ret);
    }
    {
        string prefix = "1970-01-01-08-00-00.000050_0000000000000060.0000000000000030";
        int64_t id;
        int64_t time;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, id, time);
        EXPECT_TRUE(ret);
        EXPECT_EQ(30, id);
        EXPECT_EQ(60, time);
    }
    {
        string prefix = "1970-01-01-08-00-00.000050_0000000000000060.0000000000000030.0000";
        int64_t id;
        int64_t time;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, id, time);
        EXPECT_TRUE(ret);
        EXPECT_EQ(30, id);
        EXPECT_EQ(60, time);
    }
    {
        string prefix = "1970-01-01-08-00-00.000050.0000000000000030.0000";
        int64_t id;
        int64_t time;
        bool ret = FileManagerUtil::extractMessageIdAndTimestamp(prefix, id, time);
        EXPECT_TRUE(ret);
        EXPECT_EQ(30, id);
        EXPECT_EQ(50, time);
    }
}

TEST_F(FileManagerTest, testInitFromFile) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testInitFilePairVec/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);

    touchFile(dataRoot + "1970-01-01-09-00-00.000050.0000000000000000.meta", 50);
    touchFile(dataRoot + "1970-01-01-09-00-00.000050.0000000000000000.data", 50);
    touchFile(dataRoot + "1970-01-01-09-00-00.000100.0000000000000050.meta", 100);
    touchFile(dataRoot + "1970-01-01-09-00-00.000100.0000000000000050.data", 100);
    touchFile(dataRoot + "1970-01-01-09-00-00.000300.0000000000000150.meta", 300);
    touchFile(dataRoot + "1970-01-01-09-00-00.000300.0000000000000150.data", 300);

    FileManager fileManage;
    ObsoleteFileCriterion obsoleteFileCriterion;
    EXPECT_EQ(protocol::ERROR_NONE, fileManage.init(dataRoot, obsoleteFileCriterion));
    FilePairVec filePairVec = fileManage._filePairVec;
    EXPECT_EQ((size_t)3, filePairVec.size());

    EXPECT_EQ((int64_t)0, filePairVec[0]->beginMessageId);
    EXPECT_EQ((int64_t)50, filePairVec[0]->getEndMessageId());
    EXPECT_EQ(string(dataRoot + "1970-01-01-09-00-00.000050.0000000000000000.data"), filePairVec[0]->dataFileName);
    EXPECT_EQ(string(dataRoot + "1970-01-01-09-00-00.000050.0000000000000000.meta"), filePairVec[0]->metaFileName);

    EXPECT_EQ((int64_t)50, filePairVec[1]->beginMessageId);
    EXPECT_EQ((int64_t)150, filePairVec[1]->getEndMessageId());
    EXPECT_EQ(string(dataRoot + "1970-01-01-09-00-00.000100.0000000000000050.data"), filePairVec[1]->dataFileName);
    EXPECT_EQ(string(dataRoot + "1970-01-01-09-00-00.000100.0000000000000050.meta"), filePairVec[1]->metaFileName);

    EXPECT_EQ((int64_t)150, filePairVec[2]->beginMessageId);
    EXPECT_EQ((int64_t)450, filePairVec[2]->getEndMessageId());
    EXPECT_EQ(string(dataRoot + "1970-01-01-09-00-00.000300.0000000000000150.data"), filePairVec[2]->dataFileName);
    EXPECT_EQ(string(dataRoot + "1970-01-01-09-00-00.000300.0000000000000150.meta"), filePairVec[2]->metaFileName);

    FileSystem::remove(dataRoot);
}

TEST_F(FileManagerTest, testInitFromFileMultiDfsRoot) {
    string dataRoot1 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_1/";
    string dataRoot2 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_2/";
    string dataRoot3 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_3/";

    FileSystem::remove(dataRoot1);
    FileSystem::mkDir(dataRoot1);
    FileSystem::remove(dataRoot2);
    FileSystem::mkDir(dataRoot2);
    FileSystem::remove(dataRoot3);
    FileSystem::mkDir(dataRoot3);

    touchFile(dataRoot1 + "1970-01-01-09-00-00.000050.0000000000000000.meta", 50);
    touchFile(dataRoot1 + "1970-01-01-09-00-00.000050.0000000000000000.data", 50);
    touchFile(dataRoot2 + "1970-01-01-09-00-00.000100.0000000000000050.meta", 100);
    touchFile(dataRoot2 + "1970-01-01-09-00-00.000100.0000000000000050.data", 100);
    touchFile(dataRoot3 + "1970-01-01-09-00-00.000300.0000000000000150.meta", 300);
    touchFile(dataRoot3 + "1970-01-01-09-00-00.000300.0000000000000150.data", 300);

    FileManager fileManage;
    ObsoleteFileCriterion obsoleteFileCriterion;
    vector<string> oldDfsRootVec;
    oldDfsRootVec.push_back(dataRoot1);
    oldDfsRootVec.push_back(dataRoot2);

    util::InlineVersion inlineVersion;
    EXPECT_EQ(protocol::ERROR_NONE, fileManage.init(dataRoot3, oldDfsRootVec, obsoleteFileCriterion, inlineVersion));
    FilePairVec filePairVec = fileManage._filePairVec;
    EXPECT_EQ((size_t)3, filePairVec.size());
    for (auto filePair : filePairVec) {
        EXPECT_FALSE(filePair->isAppending());
    }
    EXPECT_EQ((int64_t)0, filePairVec[0]->beginMessageId);
    EXPECT_EQ((int64_t)50, filePairVec[0]->getEndMessageId());
    EXPECT_EQ(string(dataRoot1 + "1970-01-01-09-00-00.000050.0000000000000000.data"), filePairVec[0]->dataFileName);
    EXPECT_EQ(string(dataRoot1 + "1970-01-01-09-00-00.000050.0000000000000000.meta"), filePairVec[0]->metaFileName);

    EXPECT_EQ((int64_t)50, filePairVec[1]->beginMessageId);
    EXPECT_EQ((int64_t)150, filePairVec[1]->getEndMessageId());
    EXPECT_EQ(string(dataRoot2 + "1970-01-01-09-00-00.000100.0000000000000050.data"), filePairVec[1]->dataFileName);
    EXPECT_EQ(string(dataRoot2 + "1970-01-01-09-00-00.000100.0000000000000050.meta"), filePairVec[1]->metaFileName);

    EXPECT_EQ((int64_t)150, filePairVec[2]->beginMessageId);
    EXPECT_EQ((int64_t)450, filePairVec[2]->getEndMessageId());
    EXPECT_EQ(string(dataRoot3 + "1970-01-01-09-00-00.000300.0000000000000150.data"), filePairVec[2]->dataFileName);
    EXPECT_EQ(string(dataRoot3 + "1970-01-01-09-00-00.000300.0000000000000150.meta"), filePairVec[2]->metaFileName);

    FileSystem::remove(dataRoot1);
    FileSystem::remove(dataRoot2);
    FileSystem::remove(dataRoot3);
}

TEST_F(FileManagerTest, testInitFromFileWithNewFileFormat) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testInitFilePairVec/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);

    touchFile(dataRoot + "1970-01-01-08-00-00.000000_0000000000000000.0000000000000000.meta", 50);
    touchFile(dataRoot + "1970-01-01-08-00-00.000000_0000000000000000.0000000000000000.data", 50);
    touchFile(dataRoot + "1970-01-01-08-00-00.000100.0000000000000050.meta", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000100.0000000000000050.data", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000300_0000000000000300.0000000000000150.meta", 300);
    touchFile(dataRoot + "1970-01-01-08-00-00.000300_0000000000000300.0000000000000150.data", 300);
    touchFile(dataRoot + "1970-01-01-08-00-00.000700.0000000000000450.0000000000000700.meta", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000700.0000000000000450.0000000000000700.data", 100);

    FileManager fileManage;
    ObsoleteFileCriterion obsoleteFileCriterion;
    EXPECT_EQ(protocol::ERROR_NONE, fileManage.init(dataRoot, obsoleteFileCriterion));
    FilePairVec filePairVec = fileManage._filePairVec;
    EXPECT_EQ((size_t)4, filePairVec.size());

    EXPECT_EQ((int64_t)0, filePairVec[0]->beginMessageId);
    EXPECT_EQ((int64_t)50, filePairVec[0]->getEndMessageId());
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000000_0000000000000000.0000000000000000.data"),
              filePairVec[0]->dataFileName);
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000000_0000000000000000.0000000000000000.meta"),
              filePairVec[0]->metaFileName);

    EXPECT_EQ((int64_t)50, filePairVec[1]->beginMessageId);
    EXPECT_EQ((int64_t)150, filePairVec[1]->getEndMessageId());
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000100.0000000000000050.data"), filePairVec[1]->dataFileName);
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000100.0000000000000050.meta"), filePairVec[1]->metaFileName);

    EXPECT_EQ((int64_t)150, filePairVec[2]->beginMessageId);
    EXPECT_EQ((int64_t)450, filePairVec[2]->getEndMessageId());
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000300_0000000000000300.0000000000000150.data"),
              filePairVec[2]->dataFileName);
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000300_0000000000000300.0000000000000150.meta"),
              filePairVec[2]->metaFileName);

    EXPECT_EQ((int64_t)450, filePairVec[3]->beginMessageId);
    EXPECT_EQ((int64_t)550, filePairVec[3]->getEndMessageId());
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000700.0000000000000450.0000000000000700.data"),
              filePairVec[3]->dataFileName);
    EXPECT_EQ(string(dataRoot + "1970-01-01-08-00-00.000700.0000000000000450.0000000000000700.meta"),
              filePairVec[3]->metaFileName);
    FileSystem::remove(dataRoot);
}

TEST_F(FileManagerTest, testGetObsoleteFilePos) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testGetObsoleteFilePos/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.meta", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.data", 100);

    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    obsoleteFileCriterion.reservedFileCount = 1;
    obsoleteFileCriterion.obsoleteFileTimeInterval = 1000 * 1000; // 1s
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot, obsoleteFileCriterion));
    manager.delExpiredFile();
    int64_t currTime = TimeUtility::currentTime();
    int64_t fileCreateTime = currTime - (int64_t)4000 * 1000 * 1000;
    FilePairPtr filePairPtr = manager.createNewFilePair(100, fileCreateTime);
    touchFile(filePairPtr->metaFileName, 100);
    touchFile(filePairPtr->dataFileName, 100);
    EXPECT_TRUE(filePairPtr->isAppending());

    sleep(2);
    EXPECT_EQ((int32_t)1, manager.getObsoleteFilePos(0));
    EXPECT_EQ((int32_t)0, manager.getObsoleteFilePos(1));
    EXPECT_EQ((int32_t)-1, manager.getObsoleteFilePos(2));

    // test obsolete use commitedTs
    EXPECT_EQ((int32_t)1, manager.getObsoleteFilePos(0, currTime));
    EXPECT_EQ((int32_t)0, manager.getObsoleteFilePos(1, currTime));
    EXPECT_EQ((int32_t)-1, manager.getObsoleteFilePos(2, currTime));

    sleep(1);
    FilePairPtr filePairPtr2 = manager.createNewFilePair(200, currTime);
    EXPECT_TRUE(filePairPtr2->isAppending());
    EXPECT_FALSE(filePairPtr->isAppending());
    touchFile(filePairPtr2->metaFileName, 100);
    touchFile(filePairPtr2->dataFileName, 100);
    EXPECT_EQ((int32_t)1, manager.getObsoleteFilePos(0));
    EXPECT_EQ((int32_t)1, manager.getObsoleteFilePos(1));
    EXPECT_EQ((int32_t)0, manager.getObsoleteFilePos(2));
    EXPECT_EQ((int32_t)-1, manager.getObsoleteFilePos(3));
    manager._obsoleteFileCriterion.obsoleteFileTimeInterval = (int64_t)200 * 1000 * 1000;
    EXPECT_EQ((int32_t)-1, manager.getObsoleteFilePos(0));
    EXPECT_EQ((int32_t)-1, manager.getObsoleteFilePos(1));
    EXPECT_EQ((int32_t)-1, manager.getObsoleteFilePos(2));
    EXPECT_EQ((int32_t)-1, manager.getObsoleteFilePos(3));

    // test commitedTs
    EXPECT_EQ((int32_t)1, manager.getObsoleteFilePos(0, currTime));
    EXPECT_EQ((int32_t)1, manager.getObsoleteFilePos(1, currTime));
    EXPECT_EQ((int32_t)0, manager.getObsoleteFilePos(2, currTime));
    currTime = TimeUtility::currentTime();
    EXPECT_EQ((int32_t)2, manager.getObsoleteFilePos(0, currTime));
}

FilePairPtr FileManagerTest::createFilePair(
    string metaFileName, string dataFileName, int64_t msgId, int64_t count, int64_t timeStamp, bool needTouchFile) {
    FilePairPtr filePairPtr(new FilePair(dataFileName, metaFileName, msgId, timeStamp));
    if (needTouchFile) {
        touchFile(filePairPtr->metaFileName, count);
        touchFile(filePairPtr->dataFileName, count);
    }
    //    filePairPtr->endMessageId = msgId + count;
    return filePairPtr;
}

TEST_F(FileManagerTest, testDoDeleteObsoleteFile) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testDoDeleteObsoleteFile/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot, obsoleteFileCriterion));
    manager.delExpiredFile();
    int64_t currTime = TimeUtility::currentTime();

    FilePairPtr filePairPtr = createFilePair(dataRoot + "metafile1", dataRoot + "datafile1", 100, 100, currTime);
    manager._obsoleteFilePairVec.push_back(filePairPtr);
    filePairPtr = createFilePair(dataRoot + "metafile2", dataRoot + "datafile2", 200, 100, currTime);
    manager._obsoleteFilePairVec.push_back(filePairPtr);
    filePairPtr.reset();

    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)0, manager._obsoleteFilePairVec.size());

    fslib::FileList fileList;
    EXPECT_EQ(fslib::EC_OK, FileSystem::listDir(dataRoot, fileList));
    EXPECT_EQ((size_t)0, fileList.size());

    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)0, manager._obsoleteFilePairVec.size());
    EXPECT_EQ(fslib::EC_OK, FileSystem::listDir(dataRoot, fileList));
    EXPECT_EQ((size_t)0, fileList.size());
}

TEST_F(FileManagerTest, testDoDeleteObsoleteFileWithFilePairUseCount) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testDoDeleteObsoleteFileWithFilePairUseCount/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot, obsoleteFileCriterion));
    manager.delExpiredFile();

    int64_t currTime = TimeUtility::currentTime();
    FilePairPtr filePairPtr = createFilePair(dataRoot + "metafile1", dataRoot + "datafile1", 100, 100, currTime);
    manager._obsoleteFilePairVec.push_back(filePairPtr);

    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)1, manager._obsoleteFilePairVec.size());
    fslib::FileList fileList;
    EXPECT_EQ(fslib::EC_OK, FileSystem::listDir(dataRoot, fileList));
    EXPECT_EQ((size_t)2, fileList.size());
    sort(fileList.begin(), fileList.end());
    EXPECT_EQ(string("datafile1"), fileList[0]);
    EXPECT_EQ(string("metafile1"), fileList[1]);

    filePairPtr.reset();
    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)0, manager._obsoleteFilePairVec.size());
    EXPECT_EQ(fslib::EC_OK, FileSystem::listDir(dataRoot, fileList));
    EXPECT_EQ((size_t)0, fileList.size());
}

TEST_F(FileManagerTest, testDoDeleteObsoleteFileWithRemoveException) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testDoDeleteObsoleteFileWithRemoveException/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot, obsoleteFileCriterion));
    manager.delExpiredFile();
    string dataFilePrefix1 = dataRoot + "/data1/";
    string dataFilePrefix2 = dataRoot + "/data2/";
    string metaFilePrefix1 = dataRoot + "/meta1/";
    string metaFilePrefix2 = dataRoot + "/meta2/";
    int64_t currTime = TimeUtility::currentTime();

    FilePairPtr filePairPtr1 =
        createFilePair(metaFilePrefix1 + "metafile1", dataFilePrefix1 + "datafile1", 100, 100, currTime);

    // file name too long, will remove error
    FilePairPtr filePairPtr11 =
        createFilePair(string(300, 'a'), dataFilePrefix1 + "datafile1", 100, 100, currTime, false);
    manager._obsoleteFilePairVec.push_back(filePairPtr11);
    filePairPtr11.reset();

    FilePairPtr filePairPtr2 =
        createFilePair(metaFilePrefix2 + "metafile2", dataFilePrefix2 + "datafile2", 200, 100, currTime);
    FilePairPtr filePairPtr22 =
        createFilePair(metaFilePrefix2 + "metafile2", string(300, 'a'), 200, 100, currTime, false);
    manager._obsoleteFilePairVec.push_back(filePairPtr22);
    filePairPtr22.reset();

    // job run on root, chmod lead to delete fail will not work
    // string cmd = "chmod u-w " + metaFilePrefix1;
    // system(cmd.c_str());
    // cmd = "chmod u-w " + dataFilePrefix2;
    // system(cmd.c_str());

    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)2, manager._obsoleteFilePairVec.size());
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(metaFilePrefix1 + "metafile1"));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(dataFilePrefix1 + "datafile1"));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(metaFilePrefix2 + "metafile2"));
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(dataFilePrefix2 + "datafile2"));
    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)2, manager._obsoleteFilePairVec.size());

    // cmd = "chmod u+w " + dataFilePrefix2;
    // system(cmd.c_str());
    manager._obsoleteFilePairVec[1] = filePairPtr2;
    filePairPtr2.reset();
    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)1, manager._obsoleteFilePairVec.size());
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(metaFilePrefix1 + "metafile1"));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(dataFilePrefix2 + "datafile2"));

    // cmd = "chmod u+w " + metaFilePrefix1;
    // system(cmd.c_str());
    manager._obsoleteFilePairVec[0] = filePairPtr1;
    filePairPtr1.reset();
    manager.doDeleteObsoleteFile();
    EXPECT_EQ((size_t)0, manager._obsoleteFilePairVec.size());
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(metaFilePrefix1 + "metafile1"));
}

#define CHECK_DELETE_OBSOLETE_FILE(dataRoot, filePairVecSize, obsoleteFilePairVecSize, fileListSize)                   \
    EXPECT_EQ((size_t)filePairVecSize, manager._filePairVec.size());                                                   \
    EXPECT_EQ((size_t)obsoleteFilePairVecSize, manager._obsoleteFilePairVec.size());                                   \
    EXPECT_EQ(fslib::EC_OK, FileSystem::listDir(dataRoot, fileList));                                                  \
    EXPECT_EQ((size_t)fileListSize, fileList.size());

TEST_F(FileManagerTest, testDeleteObsoleteFile) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.meta", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.data", 100);
    touchFile(dataRoot + "1970-01-01-09-00-00.000050.0000000000000100.meta", 100);
    touchFile(dataRoot + "1970-01-01-09-00-00.000050.0000000000000100.data", 100);
    touchFile(dataRoot + "1970-01-01-10-00-00.000050.0000000000000200.meta", 100);
    touchFile(dataRoot + "1970-01-01-10-00-00.000050.0000000000000200.data", 100);
    touchFile(dataRoot + "1970-01-01-11-00-00.000050.0000000000000300.meta", 100);
    touchFile(dataRoot + "1970-01-01-11-00-00.000050.0000000000000300.data", 100);
    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    fslib::FileList fileList;
    obsoleteFileCriterion.obsoleteFileTimeInterval = 500 * 1000;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot, obsoleteFileCriterion));
    manager.delExpiredFile();
    sleep(2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 4, 0, 8);

    manager._obsoleteFileCriterion.reservedFileCount = 4;
    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 4, 0, 8);

    manager._obsoleteFileCriterion.reservedFileCount = 2;
    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 2, 0, 4);
    manager._obsoleteFileCriterion.reservedFileCount = 0;

    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 1, 0, 2);

    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 1, 0, 2);
}

TEST_F(FileManagerTest, testDelExpiredFile) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.meta", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.data", 100);
    touchFile(dataRoot + "1970-01-01-09-00-00.000050.0000000000000100.meta", 100);
    touchFile(dataRoot + "1970-01-01-09-00-00.000050.0000000000000100.data", 100);
    touchFile(dataRoot + "1970-01-01-10-00-00.000050.0000000000000200.meta", 100);
    touchFile(dataRoot + "1970-01-01-10-00-00.000050.0000000000000200.data", 100);
    touchFile(dataRoot + "1970-01-01-11-00-00.000050.0000000000000300.meta", 100);
    touchFile(dataRoot + "1970-01-01-11-00-00.000050.0000000000000300.data", 100);
    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    fslib::FileList fileList;
    obsoleteFileCriterion.obsoleteFileTimeInterval = 500 * 1000;
    obsoleteFileCriterion.delObsoleteFileInterval = 1000 * 1000;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot, obsoleteFileCriterion));
    manager.delExpiredFile();
    sleep(2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 4, 0, 8);

    manager._obsoleteFileCriterion.reservedFileCount = 4;
    manager.delExpiredFile();
    sleep(2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 4, 0, 8);

    manager._obsoleteFileCriterion.reservedFileCount = 2;
    manager.delExpiredFile();
    sleep(2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 2, 0, 4);
    manager._obsoleteFileCriterion.reservedFileCount = 0;

    manager.delExpiredFile();
    sleep(2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 1, 0, 2);

    manager.delExpiredFile();
    sleep(2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot, 1, 0, 2);
}

TEST_F(FileManagerTest, testDeleteObsoleteFileMultiDfsRoot) {
    string dataRoot1 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_1/";
    string dataRoot2 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_2/";
    string dataRoot3 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_3/";

    FileSystem::remove(dataRoot1);
    FileSystem::mkDir(dataRoot1);
    FileSystem::remove(dataRoot2);
    FileSystem::mkDir(dataRoot2);
    FileSystem::remove(dataRoot3);
    FileSystem::mkDir(dataRoot3);
    touchFile(dataRoot1 + "1970-01-01-08-00-00.000050.0000000000000000.meta", 100);
    touchFile(dataRoot1 + "1970-01-01-08-00-00.000050.0000000000000000.data", 100);
    touchFile(dataRoot2 + "1970-01-01-09-00-00.000050.0000000000000100.meta", 100);
    touchFile(dataRoot2 + "1970-01-01-09-00-00.000050.0000000000000100.data", 100);
    touchFile(dataRoot2 + "1970-01-01-10-00-00.000050.0000000000000200.meta", 100);
    touchFile(dataRoot2 + "1970-01-01-10-00-00.000050.0000000000000200.data", 100);
    touchFile(dataRoot3 + "1970-01-01-11-00-00.000050.0000000000000300.meta", 100);
    touchFile(dataRoot3 + "1970-01-01-11-00-00.000050.0000000000000300.data", 100);
    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    fslib::FileList fileList;
    obsoleteFileCriterion.obsoleteFileTimeInterval = 500 * 1000;
    vector<string> oldDfsRootVec;
    oldDfsRootVec.push_back(dataRoot1);
    oldDfsRootVec.push_back(dataRoot2);
    util::InlineVersion inlineVersion;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot3, oldDfsRootVec, obsoleteFileCriterion, inlineVersion));
    manager.delExpiredFile();
    sleep(2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot1, 4, 0, 2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot2, 4, 0, 4);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot3, 4, 0, 2);

    manager._obsoleteFileCriterion.reservedFileCount = 4;
    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot1, 4, 0, 2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot2, 4, 0, 4);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot3, 4, 0, 2);

    manager._obsoleteFileCriterion.reservedFileCount = 2;
    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot1, 2, 0, 0);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot2, 2, 0, 2);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot3, 2, 0, 2);

    manager._obsoleteFileCriterion.reservedFileCount = 0;

    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot1, 1, 0, 0);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot2, 1, 0, 0);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot3, 1, 0, 2);
    manager.deleteObsoleteFile();
    CHECK_DELETE_OBSOLETE_FILE(dataRoot1, 1, 0, 0);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot2, 1, 0, 0);
    CHECK_DELETE_OBSOLETE_FILE(dataRoot3, 1, 0, 2);
    FileSystem::remove(dataRoot1);
    FileSystem::remove(dataRoot2);
    FileSystem::remove(dataRoot3);
}

#undef CHECK_DELETE_OBSOLETE_FILE

TEST_F(FileManagerTest, testSyncFilePair) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);

    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.meta", 50);
    touchFile(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.data", 50);
    touchFile(dataRoot + "1970-01-01-08-00-00.000100.0000000000000050.meta", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000100.0000000000000050.data", 100);
    touchFile(dataRoot + "1970-01-01-08-00-00.000300.0000000000000150.meta", 300);
    touchFile(dataRoot + "1970-01-01-08-00-00.000300.0000000000000150.data", 300);

    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot, obsoleteFileCriterion));
    manager.delExpiredFile();

    EXPECT_EQ(int64_t(0), manager.getFilePairById(10)->beginMessageId);
    EXPECT_EQ(int64_t(50), manager.getFilePairById(60)->beginMessageId);

    FileSystem::remove(dataRoot + "1970-01-01-08-00-00.000050.0000000000000000.meta");
    manager.deleteObsoleteFile();
    EXPECT_EQ(int64_t(50), manager.getFilePairById(60)->beginMessageId);
    FileSystem::remove(dataRoot + "1970-01-01-08-00-00.000100.0000000000000050.meta");
    manager.deleteObsoleteFile();
    EXPECT_EQ(int64_t(150), manager.getFilePairById(60)->beginMessageId);

    EXPECT_TRUE(manager.createNewFilePair(450, 400));

    EXPECT_EQ(size_t(2), manager._filePairVec.size());
    EXPECT_EQ(int64_t(150), manager._filePairVec[0]->beginMessageId);
    EXPECT_EQ(int64_t(450), manager._filePairVec[0]->getEndMessageId());
    EXPECT_EQ(int64_t(450), manager._filePairVec[1]->beginMessageId);
    EXPECT_EQ(int64_t(450), manager._filePairVec[1]->getEndMessageId());

    FileSystem::remove(dataRoot);
}

TEST_F(FileManagerTest, testSyncFilePairMultiDfsRoot) {
    string dataRoot1 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_1/";
    string dataRoot2 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_2/";
    string dataRoot3 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_3/";

    FileSystem::remove(dataRoot1);
    FileSystem::mkDir(dataRoot1);
    FileSystem::remove(dataRoot2);
    FileSystem::mkDir(dataRoot2);
    FileSystem::remove(dataRoot3);
    FileSystem::mkDir(dataRoot3);

    touchFile(dataRoot1 + "1970-01-01-08-00-00.000050.0000000000000000.meta", 50);
    touchFile(dataRoot1 + "1970-01-01-08-00-00.000050.0000000000000000.data", 50);
    touchFile(dataRoot2 + "1970-01-01-08-00-00.000100.0000000000000050.meta", 100);
    touchFile(dataRoot2 + "1970-01-01-08-00-00.000100.0000000000000050.data", 100);
    touchFile(dataRoot3 + "1970-01-01-08-00-00.000300.0000000000000150.meta", 300);
    touchFile(dataRoot3 + "1970-01-01-08-00-00.000300.0000000000000150.data", 300);

    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    vector<string> oldDfsRootVec;
    oldDfsRootVec.push_back(dataRoot1);
    oldDfsRootVec.push_back(dataRoot2);
    util::InlineVersion inlineVersion;
    EXPECT_EQ(ERROR_NONE, manager.init(dataRoot3, oldDfsRootVec, obsoleteFileCriterion, inlineVersion));
    manager.delExpiredFile();
    EXPECT_EQ(int64_t(0), manager.getFilePairById(10)->beginMessageId);
    EXPECT_EQ(int64_t(50), manager.getFilePairById(60)->beginMessageId);

    FileSystem::remove(dataRoot1 + "1970-01-01-08-00-00.000050.0000000000000000.meta");
    manager.deleteObsoleteFile();
    EXPECT_EQ(int64_t(50), manager.getFilePairById(60)->beginMessageId);
    FileSystem::remove(dataRoot2 + "1970-01-01-08-00-00.000100.0000000000000050.meta");
    manager.deleteObsoleteFile();
    EXPECT_EQ(int64_t(150), manager.getFilePairById(60)->beginMessageId);

    EXPECT_TRUE(manager.createNewFilePair(450, 400));

    EXPECT_EQ(size_t(2), manager._filePairVec.size());
    EXPECT_EQ(int64_t(150), manager._filePairVec[0]->beginMessageId);
    EXPECT_EQ(int64_t(450), manager._filePairVec[0]->getEndMessageId());
    EXPECT_EQ(int64_t(450), manager._filePairVec[1]->beginMessageId);
    EXPECT_EQ(int64_t(450), manager._filePairVec[1]->getEndMessageId());

    FileSystem::remove(dataRoot1);
    FileSystem::remove(dataRoot2);
    FileSystem::remove(dataRoot3);
}

TEST_F(FileManagerTest, testSyncFilePairMultiDfsRootWithPathNotExist) {
    string dataRoot1 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_1/";
    string dataRoot2 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_2/";
    string dataRoot3 = GET_TEMPLATE_DATA_PATH() + "testDeleteObsoleteFile_3/";

    FileSystem::remove(dataRoot1);
    FileSystem::remove(dataRoot2);
    FileSystem::mkDir(dataRoot2);
    FileSystem::remove(dataRoot3);
    FileSystem::mkDir(dataRoot3);

    touchFile(dataRoot2 + "1970-01-01-08-00-00.000100.0000000000000050.meta", 100);
    touchFile(dataRoot2 + "1970-01-01-08-00-00.000100.0000000000000050.data", 100);
    touchFile(dataRoot3 + "1970-01-01-08-00-00.000300.0000000000000150.meta", 300);
    touchFile(dataRoot3 + "1970-01-01-08-00-00.000300.0000000000000150.data", 300);

    FileManager manager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    vector<string> oldDfsRootVec;
    oldDfsRootVec.push_back(dataRoot1);
    oldDfsRootVec.push_back(dataRoot2);
    util::InlineVersion inlineVersion;
    EXPECT_EQ(ERROR_BROKER_FS_OPERATE, manager.init(dataRoot3, oldDfsRootVec, obsoleteFileCriterion, inlineVersion));
}

TEST_F(FileManagerTest, testIsTimestamp) {
    FileManager manager;
    int64_t tsStart = 1640966400; // 2022-1-1 0:0:0, 秒级时间戳
    int64_t ts = tsStart * 3600 * 1000 * 1000;
    int64_t realTime = -1;
    EXPECT_TRUE(manager.isTimestamp(ts, realTime));
    EXPECT_EQ(tsStart * 1000 * 1000, realTime);

    EXPECT_FALSE(manager.isTimestamp(ts - 1, realTime));
    EXPECT_EQ(-1, realTime);

    EXPECT_TRUE(manager.isTimestamp(ts + 1, realTime));
    EXPECT_EQ(tsStart * 1000 * 1000, realTime);

    EXPECT_TRUE(manager.isTimestamp((tsStart + 1) * 3600 * 1000 * 1000, realTime));
    EXPECT_EQ((tsStart + 1) * 1000 * 1000, realTime);

    int64_t curTime = TimeUtility::currentTimeInSeconds();
    EXPECT_FALSE(manager.isTimestamp(curTime, realTime));
    EXPECT_EQ(-1, realTime);

    EXPECT_TRUE(manager.isTimestamp(curTime * 3600 * 1000 * 1000, realTime));
    EXPECT_EQ(curTime * 1000 * 1000, realTime);
}

TEST_F(FileManagerTest, testIsFileObsolete) {
    FileManager manager;
    manager._obsoleteFileCriterion.obsoleteFileTimeInterval = 10;
    EXPECT_FALSE(manager.isFileObsolete(0, 9, FileManager::COMMITTED_TIMESTAMP_INVALID));
    EXPECT_FALSE(manager.isFileObsolete(0, 10, FileManager::COMMITTED_TIMESTAMP_INVALID));
    EXPECT_TRUE(manager.isFileObsolete(0, 11, FileManager::COMMITTED_TIMESTAMP_INVALID));

    EXPECT_FALSE(manager.isFileObsolete(1, 20, 0));
    EXPECT_FALSE(manager.isFileObsolete(1, 20, 1));
    EXPECT_TRUE(manager.isFileObsolete(1, 20, 2));
}

typedef std::function<fslib::ErrorCode(const std::string &path, const std::string &args, std::string &output)>
    ForwardFunc;
typedef void (*MockForwardFunc)(const std::string &, const ForwardFunc &);

TEST_F(FileManagerTest, testUpdateInlineVersion) {
    AUTIL_ROOT_LOG_SETLEVEL(DEBUG);
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    FileManager manager;
    manager._dataPath = "mock://na61/swift";
    manager._enableFastRecover = true;
    EXPECT_FALSE(manager._inlineVersion.valid());
    { // 1. inline file failed
        manager._inlineVersion = util::InlineVersion();
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_NOENT;
                    });
        mockForward("pangu_CreateInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        return fslib::EC_UNKNOWN;
                    });

        util::InlineVersion inlineVersion;
        EXPECT_EQ(ERROR_BROKER_READ_FILE, manager.updateInlineVersion(inlineVersion));
        EXPECT_FALSE(manager._inlineVersion.valid());
    }
    { // 2. inline file not exist, will create and set version empty
        manager._inlineVersion = util::InlineVersion();
        mockForward(
            "pangu_StatInlinefile",
            [](const string &path, const string &args, string &output) -> fslib::ErrorCode { return fslib::EC_NOENT; });
        mockForward(
            "pangu_CreateInlinefile",
            [](const string &path, const string &args, string &output) -> fslib::ErrorCode { return fslib::EC_OK; });

        util::InlineVersion inlineVersion;
        EXPECT_EQ(ERROR_NONE, manager.updateInlineVersion(inlineVersion));
        EXPECT_FALSE(manager._inlineVersion.valid());
        EXPECT_EQ(inlineVersion, manager._inlineVersion);
    }
    { // 3. inline file exist
        manager._inlineVersion = util::InlineVersion();
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        util::InlineVersion inlineVersion(0, 100);
                        output = inlineVersion.serialize();
                        return fslib::EC_OK;
                    });
        mockForward("pangu_CreateInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        return fslib::EC_PARSEFAIL;
                    });
        mockForward("pangu_UpdateInlinefileCAS",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        util::InlineVersion oldInlineVersion(0, 100);
                        util::InlineVersion newInlineVersion(0, 101);
                        std::string expectArgs = StringUtil::formatString(
                            "%s%c%s", oldInlineVersion.serialize().c_str(), '\0', newInlineVersion.serialize().c_str());
                        if (expectArgs == args) { // will set 101
                            return fslib::EC_OK;
                        } else {
                            return fslib::EC_FALSE;
                        }
                    });
        util::InlineVersion inlineVersion99(0, 99);
        util::InlineVersion inlineVersion100(0, 100);
        util::InlineVersion inlineVersion101(0, 101);
        // 3.1  input < current, return error
        EXPECT_EQ(ERROR_BROKER_INLINE_VERSION_INVALID, manager.updateInlineVersion(inlineVersion99));
        ASSERT_FALSE(manager._inlineVersion.valid());
        // 3.2  input == current, return success
        EXPECT_EQ(ERROR_NONE, manager.updateInlineVersion(inlineVersion100));
        EXPECT_EQ(util::InlineVersion(0, 100), manager._inlineVersion);
        // 3.3 input > current, update success
        EXPECT_EQ(ERROR_NONE, manager.updateInlineVersion(inlineVersion101));
        EXPECT_EQ(inlineVersion101, manager._inlineVersion);
    }
    { // 4. inline file not exist, create inline file fail
        manager._inlineVersion = util::InlineVersion();
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        output = "100";
                        return fslib::EC_NOENT;
                    });
        mockForward("pangu_CreateInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        return fslib::EC_OPERATIONTIMEOUT;
                    });
        mockForward(
            "pangu_UpdateInlinefile",
            [](const string &path, const string &args, string &output) -> fslib::ErrorCode { return fslib::EC_OK; });
        EXPECT_EQ(ERROR_BROKER_READ_FILE, manager.updateInlineVersion(util::InlineVersion(1, 1)));
        ASSERT_FALSE(manager._inlineVersion.valid());
    }
    { // 5. inline file not exist, create inline file success, update version fail
        manager._inlineVersion = util::InlineVersion();
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        util::InlineVersion inlineVersion(0, 100);
                        output = inlineVersion.serialize();
                        return fslib::EC_NOENT;
                    });
        mockForward(
            "pangu_CreateInlinefile",
            [](const string &path, const string &args, string &output) -> fslib::ErrorCode { return fslib::EC_OK; });
        mockForward("pangu_UpdateInlinefileCAS",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        return fslib::EC_OPERATIONTIMEOUT;
                    });
        EXPECT_EQ(ERROR_BROKER_FS_OPERATE, manager.updateInlineVersion(util::InlineVersion(0, 101)));
        ASSERT_FALSE(manager._inlineVersion.valid());
    }
    { // 6. inline exist, update inline file fail
        manager._inlineVersion = util::InlineVersion();
        mockForward("pangu_StatInlinefile",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        util::InlineVersion inlineVersion(0, 100);
                        output = inlineVersion.serialize();
                        return fslib::EC_OK;
                    });
        mockForward(
            "pangu_CreateInlinefile",
            [](const string &path, const string &args, string &output) -> fslib::ErrorCode { return fslib::EC_OK; });
        mockForward("pangu_UpdateInlinefileCAS",
                    [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
                        return fslib::EC_OPERATIONTIMEOUT;
                    });
        EXPECT_EQ(ERROR_BROKER_FS_OPERATE, manager.updateInlineVersion(util::InlineVersion(0, 101)));
        ASSERT_FALSE(manager._inlineVersion.valid());
    }
}

TEST_F(FileManagerTest, testSealLastFilePair) {
    string libPath = TEST_ROOT_PATH();
    setenv(FSLIB_FS_LIBSO_PATH_ENV_NAME, libPath.c_str(), 1);
    FileSystem::isFile("mock://load_so");
    auto mockModuleInfo = FileSystemFactory::getInstance()->getFsModuleInfo("mock");
    ASSERT_TRUE(mockModuleInfo.dllWrapper != nullptr);
    MockForwardFunc mockForward = (MockForwardFunc)mockModuleInfo.dllWrapper->dlsym(FSLIB_MOCK_FORWARD_FUNCTION_NAME);
    FileManager manager;
    manager._enableFastRecover = true;
    FilePairVec fpv;
    { // empty file pair
        EXPECT_EQ(ERROR_NONE, manager.sealLastFilePair(fpv));
    }
    fpv.emplace_back(new FilePair("mock://na61/1.data", "mock://na61/1.meta", 0, 100));
    fpv.emplace_back(new FilePair("mock://na61/2.data", "mock://na61/2.meta", 100, 200));
    { // seal success, only last file pair sealed
        mockForward("pangu_SealFile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
            output = "0";
            if (path.find("2.data") != string::npos || path.find("2.meta") != string::npos) {
                return fslib::EC_OK;
            } else {
                return fslib::EC_FALSE;
            }
        });
        EXPECT_EQ(ERROR_NONE, manager.sealLastFilePair(fpv));
    }
    { // seal meta error
        mockForward("pangu_SealFile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
            output = "0";
            if (path.find("meta") != string::npos) {
                return fslib::EC_NOENT;
            } else {
                return fslib::EC_OK;
            }
        });
        EXPECT_EQ(ERROR_BROKER_FS_OPERATE, manager.sealLastFilePair(fpv));
    }
    { // seal data error
        mockForward("pangu_SealFile", [](const string &path, const string &args, string &output) -> fslib::ErrorCode {
            output = "0";
            if (path.find("data") != string::npos) {
                return fslib::EC_NOENT;
            } else {
                return fslib::EC_OK;
            }
        });
        EXPECT_EQ(ERROR_BROKER_FS_OPERATE, manager.sealLastFilePair(fpv));
    }
}

TEST_F(FileManagerTest, testListFiles) {
    typedef std::vector<std::pair<std::string, std::string>> FileLists;
    MockFileManager fileManager;
    FileLists fileLists;
    EXPECT_CALL(fileManager, doListFiles(_, _)).WillOnce(Return(fslib::EC_NOTDIR));
    fslib::ErrorCode ec = fileManager.listFiles(fileLists);
    EXPECT_EQ(fslib::EC_NOTDIR, ec);
    EXPECT_EQ(0, fileLists.size());

    EXPECT_CALL(fileManager, doListFiles(_, _)).WillOnce(Return(fslib::EC_OPERATIONTIMEOUT));
    ec = fileManager.listFiles(fileLists);
    EXPECT_EQ(fslib::EC_OPERATIONTIMEOUT, ec);
    EXPECT_EQ(0, fileLists.size());

    FileManagerUtil::FileLists emptyFileLists;
    FileManagerUtil::FileLists expectFileLists;
    expectFileLists.push_back({"", "file1"});
    expectFileLists.push_back({"", "file2"});
    expectFileLists.push_back({"", "inline"});
    EXPECT_CALL(fileManager, doListFiles(_, _))
        .WillOnce(DoAll(SetArgReferee<1>(expectFileLists), Return(fslib::EC_OK)))
        .WillOnce(DoAll(Return(fslib::EC_OK)));
    ec = fileManager.listFiles(fileLists);
    EXPECT_EQ(fslib::EC_OK, ec);
    EXPECT_EQ(2, fileLists.size());

    {
        fileLists.clear();
        InSequence dummy;
        fileManager._extendDataPathVec.push_back("casualPath");
        fileManager._extendDataPathVec.push_back("casualPath");
        EXPECT_CALL(fileManager, doListFiles(_, _)).WillOnce(Return(fslib::EC_OK)).WillOnce(Return(fslib::EC_KUAFU));
        ec = fileManager.listFiles(fileLists);
        EXPECT_EQ(fslib::EC_KUAFU, ec);
        EXPECT_EQ(0, fileLists.size());
    }
    {
        InSequence dummy;
        EXPECT_CALL(fileManager, doListFiles(_, _))
            .WillOnce(Return(fslib::EC_OK))
            .WillOnce(DoAll(SetArgReferee<1>(expectFileLists), Return(fslib::EC_OK)));
        ec = fileManager.listFiles(fileLists);
        EXPECT_EQ(fslib::EC_OK, ec);
        EXPECT_EQ(2, fileLists.size());
    }
    {
        InSequence dummy;
        fileLists.clear();
        EXPECT_CALL(fileManager, doListFiles(_, _)).WillOnce(Return(fslib::EC_OPERATIONTIMEOUT));
        ec = fileManager.listFiles(fileLists);
        EXPECT_EQ(fslib::EC_OPERATIONTIMEOUT, ec);
        EXPECT_EQ(0, fileLists.size());
    }
}

} // namespace storage
} // namespace swift
