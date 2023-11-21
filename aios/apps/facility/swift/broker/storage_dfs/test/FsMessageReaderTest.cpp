#include "swift/broker/storage_dfs/FsMessageReader.h"

#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/storage_dfs/FileCache.h"
#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/broker/storage_dfs/test/FakeMessageCommitter.h"
#include "swift/common/Common.h"
#include "swift/common/FilePair.h"
#include "swift/common/MemoryMessage.h"
#include "swift/config/PartitionConfig.h"
#include "swift/filter/MessageFilter.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/PermissionCenter.h"
#include "swift/util/ReaderRec.h"
#include "swift/util/TimeoutChecker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace fslib::cache;

using namespace swift::protocol;
using namespace swift::monitor;
using namespace swift::util;
using namespace swift::common;
using namespace swift::filter;
namespace swift {
namespace storage {

class FsMessageReaderTest : public TESTBASE {
public:
    void setUp() {
        _msgCount = 1003;
        _fileManager = NULL;
        _msgCommitter = NULL;
        _amonMetricsReporter = new BrokerMetricsReporter();
        _permissionCenter = new PermissionCenter();
        _timeoutChecker = new TimeoutChecker();
        _centerPool = new BlockPool(102400, 10240);
        _fileCachePool = new BlockPool(_centerPool, 51200, 20480);
        _partitionId.set_topicname("test_topic");
        _partitionId.set_id(0);
        prepareMsgCommitter();
        int64_t filesize = 3329;
        _msgCommitter->setMaxFileSize(filesize);
        ASSERT_EQ(ERROR_NONE, _msgCommitter->write(prepareMemoryMessage(0, _msgCount)));
        DELETE_AND_SET_NULL(_msgCommitter);
        DELETE_AND_SET_NULL(_fileManager);
        _timeoutChecker->setExpireTimeout(1000 * 1000 * 1000);
    }
    void tearDown() {
        DELETE_AND_SET_NULL(_timeoutChecker);
        DELETE_AND_SET_NULL(_fileCachePool);
        DELETE_AND_SET_NULL(_centerPool);
        DELETE_AND_SET_NULL(_permissionCenter);
        DELETE_AND_SET_NULL(_amonMetricsReporter);
    }

private:
    MemoryMessageVector prepareMemoryMessage(int64_t startId, int64_t msgCount);
    void innerTestGetMessage(FsMessageReader &reader,
                             int64_t startId,
                             int64_t count,
                             int retMsgCount,
                             int64_t nextMsgId,
                             int64_t maxTotalSize = 67108864,
                             uint16_t from = 0,
                             uint16_t to = 65535,
                             uint8_t mask = 0,
                             uint8_t maskResult = 0);
    void prepareMsgCommitter();
    void checkMemoryMessage(int64_t startMsgId, protocol::MessageResponse msgs);
    void assertResultForReadFromCache(const std::string &fileName,
                                      int64_t fileLen,
                                      int64_t beginPos,
                                      FsMessageReader &fsMessageReader,
                                      int64_t fileBlockSize,
                                      int64_t expectReadLen,
                                      int64_t resultReadLen,
                                      bool isHit,
                                      int64_t getTime,
                                      int64_t hitTime);

private:
    int64_t _msgCount;
    monitor::BrokerMetricsReporter *_amonMetricsReporter;
    util::PermissionCenter *_permissionCenter;
    FileManager *_fileManager;
    FakeMessageCommitter *_msgCommitter;
    util::TimeoutChecker *_timeoutChecker;
    util::BlockPool *_centerPool;
    util::BlockPool *_fileCachePool;
    PartitionId _partitionId;
};

void FsMessageReaderTest::prepareMsgCommitter() {
    fslib::ErrorCode ec;
    ec = fslib::fs::FileSystem::remove(GET_TEMPLATE_DATA_PATH());
    EXPECT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
    ec = fslib::fs::FileSystem::mkDir(GET_TEMPLATE_DATA_PATH(), true);
    EXPECT_TRUE(ec == fslib::EC_OK);

    config::PartitionConfig config;
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    DELETE_AND_SET_NULL(_msgCommitter);
    DELETE_AND_SET_NULL(_fileManager);
    _fileManager = new FileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    EXPECT_EQ(ERROR_NONE, _fileManager->init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion));
    _msgCommitter = new FakeMessageCommitter(_partitionId, config, _fileManager, _amonMetricsReporter);
}

MemoryMessageVector FsMessageReaderTest::prepareMemoryMessage(int64_t startId, int64_t msgCount) {
    MemoryMessageVector mmv;
    for (int i = 0; i < msgCount; ++i) {
        MemoryMessage tmp;
        int64_t msgId = startId + i;
        tmp.setMsgId(msgId);
        tmp.setTimestamp(100 + msgId * 2);
        tmp.setPayload(msgId);
        tmp.setMaskPayload(msgId % 256);
        ostringstream oss;
        oss << "data." << setw(5) << setfill('0') << msgId;
        string data = oss.str();
        BlockPtr block(new Block(data.size(), NULL));
        memcpy(block->getBuffer(), data.c_str(), data.size());
        tmp.setBlock(block);
        tmp.setLen(data.size());
        tmp.setOffset(0);
        mmv.push_back(tmp);
    }
    return mmv;
}

void FsMessageReaderTest::checkMemoryMessage(int64_t startMsgId, MessageResponse msgs) {
    int msgCount = msgs.msgs_size();
    for (int i = 0; i < msgCount; ++i) {
        int64_t id = startMsgId + i;
        const Message &msg = msgs.msgs(i);
        ASSERT_EQ(id, msg.msgid());
        ASSERT_EQ(uint32_t(id), msg.uint16payload());
        ASSERT_EQ(uint32_t(id % 256), msg.uint8maskpayload());
        ASSERT_EQ(100 + id * 2, msg.timestamp());
        ostringstream oss;
        oss << "data." << setw(5) << setfill('0') << id;
        string expectData = oss.str();
        ASSERT_EQ(expectData, msg.data());
    }
}

void FsMessageReaderTest::innerTestGetMessage(FsMessageReader &reader,
                                              int64_t startId,
                                              int64_t count,
                                              int retMsgCount,
                                              int64_t nextMsgId,
                                              int64_t maxTotalSize,
                                              uint16_t from,
                                              uint16_t to,
                                              uint8_t mask,
                                              uint8_t maskResult) {
    {
        MessageResponse response;
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(false);
        request.set_startid(startId);
        request.set_count(count);
        request.set_maxtotalsize(maxTotalSize);
        request.mutable_filter()->set_from(from);
        request.mutable_filter()->set_to(to);
        request.mutable_filter()->set_uint8filtermask(mask);
        request.mutable_filter()->set_uint8maskresult(maskResult);

        stringstream ss;
        ss << "startId: " << startId << " count: " << count << " retCount: " << retMsgCount << " nextId: " << nextMsgId
           << " maxSize: " << maxTotalSize << " from: " << from << " to: " << to << " mask: " << mask
           << " maskResult: " << maskResult << endl;
        ASSERT_EQ(ERROR_NONE, reader.getMessage(&request, &response, _timeoutChecker)) << ss.str();
        ASSERT_EQ(retMsgCount, response.msgs().size()) << ss.str();
        ASSERT_EQ(nextMsgId, response.nextmsgid()) << ss.str();
    }
    {
        MessageResponse response;
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(startId);
        request.set_count(count);
        request.set_maxtotalsize(maxTotalSize);
        request.mutable_filter()->set_from(from);
        request.mutable_filter()->set_to(to);
        request.mutable_filter()->set_uint8filtermask(mask);
        request.mutable_filter()->set_uint8maskresult(maskResult);

        stringstream ss;
        ss << "startId: " << startId << " count: " << count << " retCount: " << retMsgCount << " nextId: " << nextMsgId
           << " maxSize: " << maxTotalSize << " from: " << from << " to: " << to << " mask: " << mask
           << " maskResult: " << maskResult << endl;
        ASSERT_EQ(ERROR_NONE, reader.getMessage(&request, &response, _timeoutChecker)) << ss.str();

        ASSERT_EQ(0, response.msgs().size()) << ss.str();
        ASSERT_TRUE(response.has_fbmsgs());
        FBMessageReader reader;
        ASSERT_TRUE(reader.init(response.fbmsgs()));
        ASSERT_EQ(retMsgCount, reader.size()) << ss.str();
        ASSERT_EQ(nextMsgId, response.nextmsgid()) << ss.str();
    }
}

TEST_F(FsMessageReaderTest, testGetLastMessages) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
    ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
    MessageResponse lastMsgs;
    ec = reader.getLastMessage(&lastMsgs);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(1, lastMsgs.msgs_size());
    checkMemoryMessage((int64_t)1002, lastMsgs);
}

TEST_F(FsMessageReaderTest, testGetMessageAndGetIdByTime) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
    ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
    {
        FsMessageReader reader(
            _partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
        MessageResponse msgs;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        ec = reader.getMessage(&request, &msgs, _timeoutChecker);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ((int)333, msgs.msgs_size()); // only read one file, which contain 3329/10 = 333
        checkMemoryMessage((int64_t)0, msgs);
        for (int i = 0; i < 1003; i++) {
            int64_t id = i;
            protocol::MessageIdResponse res;
            ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + id * 2 - 1, &res, _timeoutChecker);
            ASSERT_EQ(ERROR_NONE, getMinEc);
            ASSERT_EQ(100 + id * 2, res.timestamp());
        }
    }
    { // block length limit
        ReaderDes des("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info(new ReaderInfo());
        ReaderInfoMap readerInfoMap, r2;
        readerInfoMap[des] = info;
        BlockPool fileCachePool(10240, 1024);
        FsMessageReader reader(_partitionId, &fileManager, &fileCachePool, 1, 600, NULL, _amonMetricsReporter);
        MessageResponse msgs;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ((int)64, msgs.msgs_size()); // only read one block, 1024/16 = 64
        checkMemoryMessage((int64_t)0, msgs);
        for (int i = 0; i < 1003; i++) {
            int64_t id = i;
            protocol::MessageIdResponse res;
            reader.recycle(&r2, -2, -2);
            ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + id * 2 - 1, &res, _timeoutChecker);
            ASSERT_EQ(ERROR_NONE, getMinEc);
            ASSERT_EQ(i, res.msgid());
            if (i == 128 || i == 256 || i == 333 + 128 || i == 333 + 256 || // in next block
                i == 666 + 128 || i == 666 + 256) {
                ASSERT_EQ(100 + id * 2 - 1, res.timestamp());
            } else {
                ASSERT_EQ(100 + id * 2, res.timestamp());
            }
        }
        protocol::MessageIdResponse res;
        ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + 1003 * 2 + 1, &res, _timeoutChecker);
        ASSERT_EQ(ERROR_BROKER_TIMESTAMP_TOO_LATEST, getMinEc);
    }
    { // block length limit
        ReaderDes des("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info(new ReaderInfo());
        ReaderInfoMap readerInfoMap, r2;
        readerInfoMap[des] = info;
        BlockPool fileCachePool(1024, 16);
        FsMessageReader reader(_partitionId, &fileManager, &fileCachePool, 1, 600, NULL, _amonMetricsReporter);
        MessageResponse msgs;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);

        ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ((int)1, msgs.msgs_size()); // only read one block, 16/16 = 1
        checkMemoryMessage((int64_t)0, msgs);
        for (int i = 0; i < 1003; i++) {
            int64_t id = i;
            protocol::MessageIdResponse res;
            reader.recycle(&r2, -2, -2);
            ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + id * 2 - 1, &res, _timeoutChecker);
            ASSERT_EQ(ERROR_NONE, getMinEc);
            ASSERT_EQ(i, res.msgid());
            ASSERT_TRUE(100 + id * 2 - 1 == res.timestamp() || 100 + id * 2 == res.timestamp());
        }
        protocol::MessageIdResponse res;
        ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + 1003 * 2 + 1, &res, _timeoutChecker);
        ASSERT_EQ(ERROR_BROKER_TIMESTAMP_TOO_LATEST, getMinEc);
    }
    { // block length limit
        ReaderDes des("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info(new ReaderInfo());
        ReaderInfoMap readerInfoMap, r2;
        readerInfoMap[des] = info;
        BlockPool fileCachePool(1024, 32);
        FsMessageReader reader(_partitionId, &fileManager, &fileCachePool, 1, 600, NULL, _amonMetricsReporter);
        MessageResponse msgs;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ((int)2, msgs.msgs_size()); // only read one block, 16/16 = 1
        checkMemoryMessage((int64_t)0, msgs);
        for (int i = 0; i < 1003; i++) {
            int64_t id = i;
            protocol::MessageIdResponse res;
            reader.recycle(&r2, -2, -2);
            ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + id * 2 - 1, &res, _timeoutChecker);
            ASSERT_EQ(ERROR_NONE, getMinEc);
            ASSERT_EQ(i, res.msgid());
            ASSERT_TRUE(100 + id * 2 - 1 == res.timestamp() || 100 + id * 2 == res.timestamp());
        }
        protocol::MessageIdResponse res;
        ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + 1003 * 2 + 1, &res, _timeoutChecker);
        ASSERT_EQ(ERROR_BROKER_TIMESTAMP_TOO_LATEST, getMinEc);
    }
    { // block length limit
        ReaderDes des("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info(new ReaderInfo());
        ReaderInfoMap readerInfoMap, r2;
        readerInfoMap[des] = info;
        BlockPool fileCachePool(1024, 48);
        FsMessageReader reader(_partitionId, &fileManager, &fileCachePool, 1, 600, NULL, _amonMetricsReporter);
        MessageResponse msgs;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        reader.recycle(&readerInfoMap, 1, 1);
        ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ((int)3, msgs.msgs_size()); // only read one block, 48/16 = 3
        checkMemoryMessage((int64_t)0, msgs);
        for (int i = 0; i < 1003; i++) {
            int64_t id = i;
            protocol::MessageIdResponse res;
            reader.recycle(&r2, -2, -2);
            ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + id * 2 - 1, &res, _timeoutChecker);
            ASSERT_EQ(ERROR_NONE, getMinEc);
            ASSERT_EQ(i, res.msgid());
            ASSERT_TRUE(100 + id * 2 - 1 == res.timestamp() || 100 + id * 2 == res.timestamp());
        }
        protocol::MessageIdResponse res;
        ErrorCode getMinEc = reader.getMinMessageIdByTime(100 + 1003 * 2 + 1, &res, _timeoutChecker);
        ASSERT_EQ(ERROR_BROKER_TIMESTAMP_TOO_LATEST, getMinEc);
    }
}

TEST_F(FsMessageReaderTest, testReadDataFileFailed1) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
    string dataName = GET_TEMPLATE_DATA_PATH() + "/1970-01-01-08-00-00.000766.0000000000000333.0000000000000766.data";
    MessageResponse msgs;
    ConsumptionRequest request;
    request.set_startid(333);
    request.set_count(1200);
    // open file failed
    string mvCmd = "mv -f " + dataName + " " + dataName + ".tmp";
    cout << mvCmd << endl;
    (void)system(mvCmd.c_str());
    ec = reader.getMessage(&request, &msgs, _timeoutChecker);
    ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    ASSERT_EQ(0, msgs.msgs_size());
    ASSERT_EQ((int64_t)333, msgs.nextmsgid());
    msgs.Clear();
    string recover = "mv -f " + dataName + ".tmp " + dataName;
    cout << recover << endl;
    (void)system(recover.c_str());
    reader._fileCache->recycleFile(); // recycle bad file
    ec = reader.getMessage(&request, &msgs, _timeoutChecker);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ((int)333, msgs.msgs_size());
    ASSERT_EQ((int64_t)666, msgs.nextmsgid());
    checkMemoryMessage((int64_t)333, msgs);
}

TEST_F(FsMessageReaderTest, testReadDataFileFailedEmptyFile) {
    // no data, read data failed
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);

    ReaderDes des("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info(new ReaderInfo());
    ReaderInfoMap readerInfoMap;
    readerInfoMap[des] = info;

    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
    string dataName = GET_TEMPLATE_DATA_PATH() + "/1970-01-01-08-00-00.000766.0000000000000333.0000000000000766.data";
    MessageResponse msgs;
    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(1200);
    string cpCmd = "cp -f " + dataName + " " + dataName + ".tmp";
    (void)system(cpCmd.c_str());
    string catCmd = "> " + dataName; // empty file
    (void)system(catCmd.c_str());
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(333, msgs.msgs_size());
    ASSERT_EQ((int64_t)333, msgs.nextmsgid());
    checkMemoryMessage((int64_t)0, msgs);
    for (int i = 334; i <= 666; i++) {
        request.set_startid(msgs.nextmsgid());
        msgs.Clear();
        ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
        ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
        ASSERT_EQ(0, msgs.msgs_size());
        ASSERT_EQ((int64_t)333, msgs.nextmsgid()); // next msgid not move
    }
    // read left msg
    request.set_startid(666);
    msgs.Clear();
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ((int)333, msgs.msgs_size());
    ASSERT_EQ((int64_t)999, msgs.nextmsgid());
    checkMemoryMessage((int64_t)666, msgs);

    request.set_startid(msgs.nextmsgid());
    msgs.Clear();
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ((int)_msgCount - 999, msgs.msgs_size());
    ASSERT_EQ((int64_t)_msgCount, msgs.nextmsgid());
    checkMemoryMessage((int64_t)999, msgs);

    // read all message
    reader._fileCache->recycleFile(); // remove bad file
    string recover = "mv -f " + dataName + ".tmp " + dataName;
    (void)system(recover.c_str());
    request.set_startid(333);
    msgs.Clear();
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);

    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ((int)333, msgs.msgs_size());
    ASSERT_EQ((int64_t)666, msgs.nextmsgid());
    checkMemoryMessage((int64_t)333, msgs);
}

TEST_F(FsMessageReaderTest, testGetMessageWithRange) {

    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
    ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
    Filter range;
    {
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        request.mutable_filter()->set_from(330);
        request.mutable_filter()->set_to(440);
        MessageResponse msgs;
        ec = reader.getMessage(&request, &msgs, _timeoutChecker);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(3), msgs.msgs_size());
        ASSERT_EQ((int64_t)333, msgs.nextmsgid());
        checkMemoryMessage((int64_t)330, msgs);
    }
    {
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        request.mutable_filter()->set_from(0);
        request.mutable_filter()->set_to(1200);
        MessageResponse msgs;
        ec = reader.getMessage(&request, &msgs, _timeoutChecker);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(333), msgs.msgs_size());
        ASSERT_EQ((int64_t)333, msgs.nextmsgid());
        checkMemoryMessage((int64_t)0, msgs);
    }

    { // timeout checker not failed, get from cache
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        request.mutable_filter()->set_from(0);
        request.mutable_filter()->set_to(1200);
        MessageResponse msgs;
        TimeoutChecker timeoutChecker;
        timeoutChecker.setExpireTimeout(1);
        ec = reader.getMessage(&request, &msgs, &timeoutChecker);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(333), msgs.msgs_size());
        ASSERT_EQ((int64_t)333, msgs.nextmsgid());
        checkMemoryMessage((int64_t)0, msgs);
    }
    { // timeout checker failed
        reader._fileCache->_metaBlockCache.clear();
        reader._fileCache->_dataBlockCache.clear();
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1200);
        request.mutable_filter()->set_from(0);
        request.mutable_filter()->set_to(1200);
        MessageResponse msgs;
        TimeoutChecker timeoutChecker;
        timeoutChecker.setExpireTimeout(0);
        ec = reader.getMessage(&request, &msgs, &timeoutChecker);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(0), msgs.msgs_size());
        ASSERT_EQ((int64_t)0, msgs.nextmsgid());
        ASSERT_EQ((int64_t)0, msgs.nexttimestamp());
        checkMemoryMessage((int64_t)0, msgs);
    }
}

TEST_F(FsMessageReaderTest, testGetMessageWithMaxSize) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
    ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);

    innerTestGetMessage(reader, 0, 10, 5, 5, 50);
    innerTestGetMessage(reader, 0, 10, 6, 6, 65);
    innerTestGetMessage(reader, 0, 10, 1, 1, 9);
    innerTestGetMessage(reader, 1, 10, 1, 2, 9);
    innerTestGetMessage(reader, 0, 10, 10, 10, 155);
    innerTestGetMessage(reader, 0, 2, 2, 3, 155, 1, 3);
    innerTestGetMessage(reader, 0, 10, 3, 333, 100000, 1, 3);
}

TEST_F(FsMessageReaderTest, testGetMessageWithMask) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
    ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);

    innerTestGetMessage(reader, 0, 10, 2, 333, 10000, 0, 65535, 0xff, 0x45);
    innerTestGetMessage(reader, 0, 10, 10, 84, 10000, 0, 65535, 0x07, 0x04);
    innerTestGetMessage(reader, 0, 130, 42, 333, 10000, 0, 65535, 0x07, 0x04);
    innerTestGetMessage(reader, 0, 20, 0, 333, 10000, 0, 65535, 0x08, 0x04);

    innerTestGetMessage(reader, 0, 3, 3, 76, 10000, 50, 150, 0x07, 0x04);
    innerTestGetMessage(reader, 0, 100, 13, 333, 10000, 50, 150, 0x07, 0x04);
}

TEST_F(FsMessageReaderTest, testValidateMessageId) {
    {
        FileManager fileManager;
        ObsoleteFileCriterion obsoleteFileCriterion;
        ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
        uint32_t deletedFileCount;
        fileManager.delExpiredFile(FileManager::COMMITTED_TIMESTAMP_INVALID, deletedFileCount);
        ASSERT_EQ((uint32_t)0, deletedFileCount);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
        ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
        FsMessageReader reader(
            _partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
        EXPECT_TRUE(reader.messageIdValid(0, 0, _timeoutChecker));
        EXPECT_TRUE(reader.messageIdValid(1, 100 + 1 * 2, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(1, 100, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(1, 98, _timeoutChecker));
        ASSERT_TRUE(reader.messageIdValid(_msgCount - 1, 100 + (_msgCount - 1) * 2, _timeoutChecker));
        ASSERT_FALSE(reader.messageIdValid(_msgCount - 1, 100 + (_msgCount - 1) * 2 + 1, _timeoutChecker));
        ASSERT_FALSE(reader.messageIdValid(_msgCount, 100 + _msgCount * 2, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(_msgCount, 100 + _msgCount * 2 - 2, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(_msgCount, 100 + _msgCount * 2 - 100, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(_msgCount + 1, 100 + _msgCount * 2 + 2, _timeoutChecker));
        // different file
        ASSERT_TRUE(reader.messageIdValid(333, 100 + 333 * 2, _timeoutChecker));
        ASSERT_TRUE(reader.messageIdValid(333, 100 + 333 * 2 - 1, _timeoutChecker));
        ASSERT_FALSE(reader.messageIdValid(333, 100 + 333 * 2 + 1, _timeoutChecker));
    }

    {
        FileManager fileManager;
        ObsoleteFileCriterion obsoleteFileCriterion;
        obsoleteFileCriterion.delObsoleteFileInterval = 3 * 1000 * 1000;
        obsoleteFileCriterion.reservedFileCount = 3;
        obsoleteFileCriterion.obsoleteFileTimeInterval = 1;
        ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
        ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
        FsMessageReader reader(
            _partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);

        ASSERT_TRUE(reader.messageIdValid(334, 100 + 334 * 2, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(334, 100 + 333 * 2, _timeoutChecker));
        sleep(5);
        uint32_t deletedFileCount;
        fileManager.delExpiredFile(FileManager::COMMITTED_TIMESTAMP_INVALID, deletedFileCount);
        ASSERT_EQ((uint32_t)1, deletedFileCount);
        ASSERT_TRUE(reader.messageIdValid(334, 100 + 334 * 2, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(334, 100 + 333 * 2, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(1, 100 + 1 * 2, _timeoutChecker));
        ASSERT_TRUE(!reader.messageIdValid(333, 100 + 333 * 2, _timeoutChecker));
    }
}

TEST_F(FsMessageReaderTest, testValidateMessageId_cross_file_and_block) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    prepareMsgCommitter();
    int64_t filesize = 9999; // 1000 msgs one file, 1 block 640 msgs
    _msgCommitter->setMaxFileSize(filesize);
    int64_t msgCount = 2000;
    MemoryMessageVector msgVector = prepareMemoryMessage(0, 2000);
    ASSERT_EQ(ERROR_NONE, _msgCommitter->write(msgVector));
    ASSERT_EQ(ERROR_NONE, _msgCommitter->commitFile());
    DELETE_AND_SET_NULL(_msgCommitter);
    DELETE_AND_SET_NULL(_fileManager);
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(0, fileManager.getMinMessageId());
    ASSERT_EQ(msgCount - 1, fileManager.getLastMessageId());
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
    // different file
    ASSERT_TRUE(reader.messageIdValid(1000, 100 + 1000 * 2, _timeoutChecker));
    ASSERT_TRUE(reader.messageIdValid(1000, 100 + 1000 * 2 - 1, _timeoutChecker));
    ASSERT_FALSE(reader.messageIdValid(1000, 100 + 1000 * 2 + 1, _timeoutChecker));
    // different block
    ASSERT_TRUE(reader.messageIdValid(640, 100 + 640 * 2, _timeoutChecker));
    ASSERT_TRUE(reader.messageIdValid(640, 100 + 640 * 2 - 1, _timeoutChecker));
    ASSERT_FALSE(reader.messageIdValid(640, 100 + 640 * 2 + 1, _timeoutChecker));
}

TEST_F(FsMessageReaderTest, testRecoverLastMsg) {
    { //    data file has redundant msgs: 5 complete
        prepareMsgCommitter();
        int64_t filesize = 3329;
        _msgCommitter->setMaxFileSize(filesize);
        int64_t msgCount = 5;
        MemoryMessageVector msgVector = prepareMemoryMessage(0, msgCount);
        ASSERT_EQ(ERROR_NONE, _msgCommitter->write(msgVector));
        ASSERT_EQ(ERROR_NONE, _msgCommitter->commitFile());
        msgVector = prepareMemoryMessage(msgCount, msgCount);
        for (size_t i = 0; i < msgVector.size(); ++i) {
            _msgCommitter->appendMsgForDataFile(msgVector[i], (int64_t)10000);
        }
        DELETE_AND_SET_NULL(_msgCommitter);

        DELETE_AND_SET_NULL(_fileManager);
        _fileManager = new FileManager;
        ObsoleteFileCriterion obsoleteFileCriterion;
        ASSERT_EQ(ERROR_NONE, _fileManager->init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion));
        FsMessageReader reader(
            _partitionId, _fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
        MessageResponse lastMsgs;
        ErrorCode ec = reader.getLastMessage(&lastMsgs);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(1), lastMsgs.msgs_size());
        checkMemoryMessage(4, lastMsgs);
        DELETE_AND_SET_NULL(_fileManager);
    }
    { // data file has redundant msgs: 4 complete, 1 uncompleted
        prepareMsgCommitter();
        int64_t filesize = 3329;
        _msgCommitter->setMaxFileSize(filesize);
        int64_t msgCount = 5;
        MemoryMessageVector msgVector = prepareMemoryMessage(0, msgCount);
        ASSERT_EQ(ERROR_NONE, _msgCommitter->write(msgVector));
        ASSERT_EQ(ERROR_NONE, _msgCommitter->commitFile());
        msgVector = prepareMemoryMessage(msgCount, msgCount);
        for (size_t i = 0; i < msgVector.size() - 1; ++i) {
            _msgCommitter->appendMsgForDataFile(msgVector[i], (int64_t)10000);
        }
        _msgCommitter->appendMsgForDataFile(msgVector[msgVector.size() - 1], (int64_t)4);
        DELETE_AND_SET_NULL(_msgCommitter);

        DELETE_AND_SET_NULL(_fileManager);
        _fileManager = new FileManager;
        ObsoleteFileCriterion obsoleteFileCriterion;
        ASSERT_EQ(ERROR_NONE, _fileManager->init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion));
        BlockPool fileCachePool(51200, 20480);
        FsMessageReader reader(_partitionId, _fileManager, &fileCachePool, 1, 600, NULL, _amonMetricsReporter);
        MessageResponse lastMsgs;
        ErrorCode ec = reader.getLastMessage(&lastMsgs);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(1), lastMsgs.msgs_size());
        checkMemoryMessage(4, lastMsgs);
        DELETE_AND_SET_NULL(_fileManager);
    }
    { //    meta file's last meta info is uncompleted
        prepareMsgCommitter();
        int64_t filesize = 3329;
        _msgCommitter->setMaxFileSize(filesize);
        int64_t msgCount = 5;
        MemoryMessageVector msgVector = prepareMemoryMessage(0, msgCount);
        ASSERT_EQ(ERROR_NONE, _msgCommitter->write(msgVector));
        ASSERT_EQ(ERROR_NONE, _msgCommitter->commitFile());
        msgVector = prepareMemoryMessage(msgCount, msgCount);
        for (size_t i = 0; i < msgVector.size(); ++i) {
            _msgCommitter->appendMsgForDataFile(msgVector[i], (int64_t)10000);
        }
        for (size_t i = 0; i < msgVector.size() - 1; ++i) {
            _msgCommitter->appendMsgMetaForMetaFile(msgVector[i], (int64_t)10000);
        }
        _msgCommitter->appendMsgMetaForMetaFile(msgVector[msgVector.size() - 1], (int64_t)5);
        DELETE_AND_SET_NULL(_msgCommitter);

        DELETE_AND_SET_NULL(_fileManager);
        _fileManager = new FileManager;
        ObsoleteFileCriterion obsoleteFileCriterion;
        ASSERT_EQ(ERROR_NONE, _fileManager->init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion));
        BlockPool fileCachePool(51200, 20480);
        FsMessageReader reader(_partitionId, _fileManager, &fileCachePool, 1, 600, NULL, _amonMetricsReporter);
        MessageResponse lastMsgs;
        ErrorCode ec = reader.getLastMessage(&lastMsgs);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(1), lastMsgs.msgs_size());
        checkMemoryMessage(8, lastMsgs);
        DELETE_AND_SET_NULL(_fileManager);
    }
    { // message lost
        prepareMsgCommitter();
        int64_t filesize = 3329;
        _msgCommitter->setMaxFileSize(filesize);
        int64_t msgCount = 5;
        MemoryMessageVector msgVector = prepareMemoryMessage(0, msgCount);
        ASSERT_EQ(ERROR_NONE, _msgCommitter->write(msgVector));
        ASSERT_EQ(ERROR_NONE, _msgCommitter->commitFile());
        DELETE_AND_SET_NULL(_msgCommitter);
        DELETE_AND_SET_NULL(_fileManager);
        _fileManager = new FileManager;
        ObsoleteFileCriterion obsoleteFileCriterion;
        ASSERT_EQ(ERROR_NONE, _fileManager->init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion));
        _fileManager->_filePairVec[0]->_endMessageId = 888; // actual 5
        BlockPool fileCachePool(51200, 20480);
        FsMessageReader reader(_partitionId, _fileManager, &fileCachePool, 1, 600, NULL, _amonMetricsReporter);
        MessageResponse lastMsgs;
        ErrorCode ec = reader.getLastMessage(&lastMsgs);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(int(1), lastMsgs.msgs_size());
        ASSERT_EQ(888, lastMsgs.msgs(0).msgid());
        DELETE_AND_SET_NULL(_fileManager);
    }
}

TEST_F(FsMessageReaderTest, testSyncFilePair) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int64_t(0), fileManager.getMinMessageId());
    ASSERT_EQ(int64_t(_msgCount - 1), fileManager.getLastMessageId());
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);

    ConsumptionRequest request;
    request.set_startid(0);
    request.set_count(10);

    ReaderDes des("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info(new ReaderInfo());
    ReaderInfoMap readerInfoMap;
    readerInfoMap[des] = info;

    MessageResponse msgs;
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(int(10), msgs.msgs_size());
    ASSERT_EQ(ERROR_NONE, ec);
    for (int64_t i = 0; i < 10; ++i) {
        ASSERT_EQ(int64_t(i), (int64_t)msgs.msgs(i).msgid());
    }
    string cmd = "rm " + GET_TEMPLATE_DATA_PATH() + "/" + "*100.meta .";
    cout << "--->" << cmd << endl;
    (void)system(cmd.c_str());

    uint32_t deletedFileCount;
    fileManager.deleteObsoleteFile(FileManager::COMMITTED_TIMESTAMP_INVALID, deletedFileCount);
    EXPECT_EQ((uint32_t)0, deletedFileCount);
    msgs.Clear();
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    ASSERT_EQ(int(0), msgs.msgs_size());
    ASSERT_EQ(int(333), (int)msgs.nextmsgid());
    request.set_startid(msgs.nextmsgid());
    msgs.Clear();
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int(10), msgs.msgs_size());
    for (int64_t i = 0; i < 10; ++i) {
        ASSERT_EQ(int64_t(333 + i), (int64_t)msgs.msgs(i).msgid());
    }

    msgs.Clear();
    request.set_startid(800);
    request.set_count(10);
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int(10), msgs.msgs_size());
    for (int64_t i = 0; i < 10; ++i) {
        ASSERT_EQ(int64_t(800 + i), (int64_t)msgs.msgs(i).msgid());
    }

    msgs.Clear();
    request.set_startid(600);
    request.set_count(100);
    ec = reader.getMessage(&request, &msgs, _timeoutChecker, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(int(66), msgs.msgs_size()); // 666- 600
    for (int64_t i = 0; i < 66; ++i) {
        ASSERT_EQ(int64_t(600 + i), (int64_t)msgs.msgs(i).msgid());
    }
}

TEST_F(FsMessageReaderTest, testGetMessageWithTimeout) {
    FileManager fileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ErrorCode ec = fileManager.init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion);
    ASSERT_EQ(ERROR_NONE, ec);
    FsMessageReader reader(_partitionId, &fileManager, _fileCachePool, 1, 600, _permissionCenter, _amonMetricsReporter);
    _timeoutChecker->setTimeout(true);
    protocol::MessageResponse response;
    protocol::ConsumptionRequest request;
    request.set_maxtotalsize(10 * 1024 * 1024);
    request.set_startid(0);
    ec = reader.getMessage(&request, &response, _timeoutChecker);
    ASSERT_EQ(ERROR_NONE, ec);
    EXPECT_FALSE(response.has_nexttimestamp());
}

TEST_F(FsMessageReaderTest, testNeedMakeOneEmptyMessage) {
    protocol::MessageResponse response;
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, -1));
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, 0));
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, 1));

    for (int i = 0; i < 10; ++i) {
        Message *message = response.add_msgs();
        message->set_msgid(10 + i);
    }
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, 10));
    EXPECT_FALSE(FsMessageReader::needMakeOneEmptyMessage(&response, 19));
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, 20));
    for (int i = 0; i < 10; ++i) {
        Message *message = response.add_msgs();
        message->set_msgid(100 + i);
    }
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, 118));
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, 119));
    ASSERT_TRUE(FsMessageReader::needMakeOneEmptyMessage(&response, 120));
}
} // namespace storage
} // namespace swift
