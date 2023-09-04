#include "swift/common/FilePair.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string>

#include "FakeFslibFile.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/protocol/ErrCode.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace storage {

class FilePairTest : public TESTBASE {
protected:
    void touchFile(const std::string &fileName, uint32_t count);
    void assertResultForTestSeekFslibFile(const std::string &fileName, FakeFslibFile &fakeFile);
};

TEST_F(FilePairTest, testMessageCountByMetaLen) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testMessageCountByMetaLen/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    string meta1 = dataRoot + "m1";
    FilePairPtr f1(new FilePair("d", meta1, 2, 0));
    int64_t msgCnt;
    int64_t cnt = 10;
    ErrorCode ec = f1->getMessageCountByMetaLength(msgCnt);
    EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    touchFile(meta1, cnt);
    ec = f1->getMessageCountByMetaLength(msgCnt);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(cnt, msgCnt);
}

TEST_F(FilePairTest, testMessageCountByMetaContent) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testMessageCountByMetaContent/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    string meta1 = dataRoot + "m1";
    FilePairPtr f1(new FilePair("d", meta1, 2, 0));
    int64_t msgCnt;
    int64_t cnt = 10;
    ErrorCode ec = f1->getMessageCountByMetaContent(msgCnt);
    EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    touchFile(meta1, cnt);
    ec = f1->getMessageCountByMetaContent(msgCnt);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(cnt, msgCnt);
}

TEST_F(FilePairTest, testUpdateEndMessageId) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testUpdateEndMessageId/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    string meta1 = dataRoot + "m1";
    FilePairPtr f1(new FilePair("d", meta1, 2, 0));
    ErrorCode ec = f1->updateEndMessageId();
    EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    EXPECT_EQ(int64_t(-1), f1->_endMessageId);
    int64_t cnt = 10;
    touchFile(meta1, cnt);
    ec = f1->updateEndMessageId();
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(int64_t(12), f1->_endMessageId);

    string meta2 = dataRoot + "m2";
    FilePairPtr f2(new FilePair("d", meta2, 0, 0));
    f2->setNext(f1);
    ec = f2->updateEndMessageId();
    EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    EXPECT_EQ(int64_t(-1), f2->_endMessageId);

    touchFile(meta2, 2);
    ec = f2->updateEndMessageId();
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(int64_t(2), f2->_endMessageId);

    touchFile(meta2, 1);
    ec = f2->updateEndMessageId();
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(int64_t(1), f2->_endMessageId);

    touchFile(meta2, 5);
    ec = f2->updateEndMessageId();
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(int64_t(2), f2->_endMessageId);
}

TEST_F(FilePairTest, testgetEndMessageId) {
    string dataRoot = GET_TEMPLATE_DATA_PATH() + "testGetEndMessageId/";
    FileSystem::remove(dataRoot);
    FileSystem::mkDir(dataRoot);
    string meta1 = dataRoot + "m1";
    FilePairPtr f1(new FilePair("d", meta1, 2, 0));
    int64_t msgId;
    ErrorCode ec = f1->getEndMessageId(msgId);
    EXPECT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    EXPECT_EQ(int64_t(2), msgId);
    msgId = f1->getEndMessageId();
    EXPECT_EQ(int64_t(2), msgId);

    int64_t cnt = 10;
    touchFile(meta1, cnt);
    ec = f1->getEndMessageId(msgId);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(int64_t(12), msgId);
    msgId = f1->getEndMessageId();
    EXPECT_EQ(int64_t(12), msgId);
    FileSystem::remove(dataRoot);
    ec = f1->getEndMessageId(msgId);
    EXPECT_EQ(ERROR_NONE, ec);
    EXPECT_EQ(int64_t(12), msgId);
    msgId = f1->getEndMessageId();
    EXPECT_EQ(int64_t(12), msgId);
}

void FilePairTest::touchFile(const string &fileName, uint32_t count) {
    FilePtr file(FileSystem::openFile(fileName, fslib::WRITE));
    string str(count * FILE_MESSAGE_META_SIZE, 'a');
    file->write(str.data(), str.length());
    file->close();
}

TEST_F(FilePairTest, testSeekFslibFile) {
    string fileName = GET_TEMPLATE_DATA_PATH() + "/testSeekFslibFile";
    string content(1000, 'a');
    FileSystem::writeFile(fileName, content);
    {
        File *inputFile = FileSystem::openFile(fileName, fslib::READ);
        FakeFslibFile fakeFile(inputFile);
        assertResultForTestSeekFslibFile(fileName, fakeFile);
    }

    {
        File *inputFile = FileSystem::openFile(fileName, fslib::READ);
        FakeFslibFile fakeFile(inputFile);
        fakeFile.setSeekPosLimit(0);
        assertResultForTestSeekFslibFile(fileName, fakeFile);
    }

    {
        File *inputFile = FileSystem::openFile(fileName, fslib::READ);
        FakeFslibFile fakeFile(inputFile);
        fakeFile.setSeekPosLimit(200);
        assertResultForTestSeekFslibFile(fileName, fakeFile);
    }
}

void FilePairTest::assertResultForTestSeekFslibFile(const string &fileName, FakeFslibFile &fakeFile) {
    int64_t actualSeekPos;
    EXPECT_EQ(protocol::ERROR_NONE, FilePair::seekFslibFile(fileName, &fakeFile, 0, actualSeekPos));
    EXPECT_EQ((int64_t)0, actualSeekPos);

    EXPECT_EQ(protocol::ERROR_NONE, FilePair::seekFslibFile(fileName, &fakeFile, 500, actualSeekPos));
    EXPECT_EQ((int64_t)500, actualSeekPos);

    EXPECT_EQ(protocol::ERROR_NONE, FilePair::seekFslibFile(fileName, &fakeFile, 999, actualSeekPos));
    EXPECT_EQ((int64_t)999, actualSeekPos);

    EXPECT_EQ(protocol::ERROR_NONE, FilePair::seekFslibFile(fileName, &fakeFile, 1000, actualSeekPos));
    EXPECT_EQ((int64_t)1000, actualSeekPos);

    EXPECT_EQ(protocol::ERROR_NONE, FilePair::seekFslibFile(fileName, &fakeFile, 1001, actualSeekPos));
    EXPECT_EQ((int64_t)1000, actualSeekPos);

    EXPECT_EQ(protocol::ERROR_NONE, FilePair::seekFslibFile(fileName, &fakeFile, 1500, actualSeekPos));
    EXPECT_EQ((int64_t)1000, actualSeekPos);
}

} // namespace storage
} // namespace swift
