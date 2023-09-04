#include "swift/broker/storage_dfs/BlockMessageReader.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <zlib.h>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "flatbuffers/flatbuffers.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/storage/test/MemoryMessageUtil.h"
#include "swift/broker/storage_dfs/FileCache.h"
#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/broker/storage_dfs/FileMessageMetaVec.h"
#include "swift/broker/storage_dfs/test/FakeMessageCommitter.h"
#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/common/FilePair.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/RangeUtil.h"
#include "swift/config/PartitionConfig.h"
#include "swift/filter/MessageFilter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/PermissionCenter.h"
#include "unittest/unittest.h"

namespace swift {
namespace storage {
class BlockMessageReaderTest_testFilter_Test;
class BlockMessageReaderTest_testFirstMsgLargerThanMaxTotalSize_Test;
class BlockMessageReaderTest_testMaxTotalSizeLimit_Test;
class BlockMessageReaderTest_testSimpleProcess_Test;
} // namespace storage
} // namespace swift

using namespace std;
using namespace autil;
using namespace fslib::cache;
using namespace swift::protocol;
using namespace swift::filter;
using namespace swift::util;
using namespace swift::common;

namespace swift {
namespace storage {

#define CHECK_FILE_MESSAGE(args...)                                                                                    \
    string lineCount = autil::StringUtil::toString(__LINE__);                                                          \
    checkFillMessage(lineCount, args)

class BlockMessageReaderTest : public TESTBASE {
public:
    void setUp() {
        _fileManager = NULL;
        _msgCountInBlock = 8;
        _startId = 10;
        _count = 60;
    }
    void tearDown() { DELETE_AND_SET_NULL(_fileManager); }

private:
    void
    prepareFSMessage(int64_t startId, int32_t count, uint32_t fileCnt = 1, bool compress = false, bool merge = false);
    MemoryMessageVector prepareMemoryMessage(int64_t startId, int64_t msgCount, bool compress, bool merge = false);
    void checkFillMessage(const std::string &lineCount,
                          const ConsumptionRequest *request,
                          int resultMsgCount,
                          ErrorCode ec,
                          bool compress = false,
                          bool merge = false);

private:
    FileManager *_fileManager;
    int64_t _msgCountInBlock;
    int64_t _startId = 10;
    int32_t _count = 100;
};

TEST_F(BlockMessageReaderTest, testSimpleProcess) {
    ConsumptionRequest request;
    request.set_startid(10);
    request.set_count(10);
    { CHECK_FILE_MESSAGE(&request, _msgCountInBlock, ERROR_NONE, false); }
    { CHECK_FILE_MESSAGE(&request, _msgCountInBlock, ERROR_NONE, true); }
    request.mutable_versioninfo()->set_supportfb(true);
    { CHECK_FILE_MESSAGE(&request, _msgCountInBlock, ERROR_NONE, false); }
    { CHECK_FILE_MESSAGE(&request, _msgCountInBlock, ERROR_NONE, true); }
}

TEST_F(BlockMessageReaderTest, testFirstMsgLargerThanMaxTotalSize) {
    ConsumptionRequest request;
    request.set_startid(10);
    request.set_count(10);
    request.set_maxtotalsize(1);
    CHECK_FILE_MESSAGE(&request, 1, ERROR_NONE);
}

TEST_F(BlockMessageReaderTest, testMaxTotalSizeLimit) {
    {
        ConsumptionRequest request;
        request.set_startid(50);
        request.set_count(10);
        request.set_maxtotalsize(125);
        int resultMsgCount = 2;
        CHECK_FILE_MESSAGE(&request, resultMsgCount, ERROR_NONE, false);
    }
    {
        ConsumptionRequest request;
        request.set_startid(50);
        request.set_count(10);
        request.set_maxtotalsize(126);
        int resultMsgCount = 3;
        CHECK_FILE_MESSAGE(&request, resultMsgCount, ERROR_NONE);
    }
    {
        ConsumptionRequest request;
        request.set_startid(50);
        request.set_count(10);
        request.set_maxtotalsize(127);
        int resultMsgCount = 3;
        CHECK_FILE_MESSAGE(&request, resultMsgCount, ERROR_NONE);
    }
}

TEST_F(BlockMessageReaderTest, testFilter) {
    ConsumptionRequest request;
    request.set_startid(50);
    request.set_count(10);
    request.set_maxtotalsize(10000);
    request.mutable_filter()->set_from(53);
    request.mutable_filter()->set_to(63);
    int resultMsgCount = 5; // one block
    CHECK_FILE_MESSAGE(&request, resultMsgCount, ERROR_NONE);
}

TEST_F(BlockMessageReaderTest, testReadNotExistFile) {
    prepareFSMessage(_startId, _count, 1);
    FilePairPtr filePair = _fileManager->getFilePairById(_startId);
    assert(filePair);
    string cmdRmFile = "rm -rf " + filePair->dataFileName;
    (void)system(cmdRmFile.c_str());

    int64_t blockSize = _msgCountInBlock * FILE_MESSAGE_META_SIZE; // 8 message in block
    int64_t fileCacheSize = 1280;                                  // 10 block
    PermissionCenter permissionCenter;
    BlockPool fileCachePool(fileCacheSize, blockSize);
    FileCache fileCache(&fileCachePool, 1, 600, &permissionCenter);
    BlockMessageReader reader(filePair, &fileCache, NULL);
    ConsumptionRequest request;
    request.set_startid(50);
    request.set_count(10);
    protocol::MessageResponse response;
    ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, reader.fillMessage(&request, &response));
}

TEST_F(BlockMessageReaderTest, testReadBadDataFile) {
    prepareFSMessage(_startId, _count, 1);
    FilePairPtr filePair = _fileManager->getFilePairById(_startId);
    assert(filePair);
    int64_t blockSize = _msgCountInBlock * FILE_MESSAGE_META_SIZE; // 8 message in block
    int64_t fileCacheSize = 1280;                                  // 10 block
    PermissionCenter permissionCenter;
    BlockPool fileCachePool(fileCacheSize, blockSize);
    FileCache fileCache(&fileCachePool, 1, 600, &permissionCenter, NULL);
    string cmdCpFile = "cp " + filePair->dataFileName + " " + filePair->dataFileName + "_bak";
    (void)system(cmdCpFile.c_str());
    string cmdRmFile = "echo 0 >  " + filePair->dataFileName;
    (void)system(cmdRmFile.c_str());
    ConsumptionRequest request;
    request.set_startid(50);
    request.set_count(10);
    protocol::MessageResponse response;
    BlockMessageReader reader(filePair, &fileCache, NULL, NULL, NULL);
    ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, reader.fillMessage(&request, &response));

    cmdRmFile = "rm -f " + filePair->dataFileName;
    (void)system(cmdRmFile.c_str());
    string cmdMvFile = "mv " + filePair->dataFileName + "_bak " + filePair->dataFileName;
    (void)system(cmdMvFile.c_str());
    ASSERT_EQ(ERROR_NONE, reader.fillMessage(&request, &response));
}

TEST_F(BlockMessageReaderTest, testReadBadMetaFile) {
    prepareFSMessage(_startId, _count, 1);
    FilePairPtr filePair = _fileManager->getFilePairById(_startId);
    assert(filePair);
    int64_t blockSize = _msgCountInBlock * FILE_MESSAGE_META_SIZE; // 8 message in block
    int64_t fileCacheSize = 1280;                                  // 10 block
    PermissionCenter permissionCenter;
    BlockPool fileCachePool(fileCacheSize, blockSize);
    FileCache fileCache(&fileCachePool, 1, 600, &permissionCenter, NULL);
    string cmdCpFile = "cp " + filePair->metaFileName + " " + filePair->metaFileName + "_bak";
    (void)system(cmdCpFile.c_str());
    string cmdRmFile = "echo 0 >  " + filePair->metaFileName;
    (void)system(cmdRmFile.c_str());
    ConsumptionRequest request;
    request.set_startid(50);
    request.set_count(10);
    protocol::MessageResponse response;
    BlockMessageReader reader(filePair, &fileCache, NULL, NULL, NULL);
    ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, reader.fillMessage(&request, &response));

    cmdRmFile = "rm -f " + filePair->metaFileName;
    (void)system(cmdRmFile.c_str());
    string cmdMvFile = "mv " + filePair->metaFileName + "_bak " + filePair->metaFileName;
    (void)system(cmdMvFile.c_str());
    ASSERT_EQ(ERROR_NONE, reader.fillMessage(&request, &response));
}

TEST_F(BlockMessageReaderTest, testReadMergeMessage) {
    prepareFSMessage(0, 3, 1, false, true);
    FilePairPtr filePair = _fileManager->getFilePairById(0);
    assert(filePair);
    int64_t blockSize = _msgCountInBlock * FILE_MESSAGE_META_SIZE; // 8 message in block
    int64_t fileCacheSize = 1280;                                  // 10 block
    PermissionCenter permissionCenter;
    BlockPool fileCachePool(fileCacheSize, blockSize);
    FileCache fileCache(&fileCachePool, 1, 600, &permissionCenter, NULL);
    BlockMessageReader reader(filePair, &fileCache, NULL);
    {
        protocol::MessageResponse response;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(3);
        ASSERT_EQ(ERROR_NONE, reader.fillMessage(&request, &response));
        ASSERT_EQ(2, response.msgs_size());
        ASSERT_TRUE(response.msgs(0).merged());
        ASSERT_TRUE(!response.msgs(1).merged());
        ASSERT_EQ(3, response.totalmsgcount());
    }
    {
        protocol::MessageResponse response;
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(3);
        ASSERT_EQ(ERROR_NONE, reader.fillMessage(&request, &response));
        protocol::FBMessageReader fbReader;
        ASSERT_TRUE(response.messageformat() == MF_FB);
        ASSERT_TRUE(fbReader.init(response.fbmsgs(), false));
        ASSERT_EQ(2, fbReader.size());
        ASSERT_TRUE(fbReader.read(0)->merged());
        ASSERT_TRUE(!fbReader.read(1)->merged());
        ASSERT_EQ(3, response.totalmsgcount());
    }
    {
        protocol::MessageResponse response;
        ConsumptionRequest request;
        request.set_startid(0);
        request.set_count(1);
        ASSERT_EQ(ERROR_NONE, reader.fillMessage(&request, &response));
        ASSERT_EQ(1, response.msgs_size());
        ASSERT_TRUE(response.msgs(0).merged());
        ASSERT_EQ(2, response.totalmsgcount());
    }
    {
        protocol::MessageResponse response;
        ConsumptionRequest request;
        request.mutable_versioninfo()->set_supportfb(true);
        request.set_startid(0);
        request.set_count(1);
        ASSERT_EQ(ERROR_NONE, reader.fillMessage(&request, &response));
        protocol::FBMessageReader fbReader;
        ASSERT_TRUE(response.messageformat() == MF_FB);
        ASSERT_TRUE(fbReader.init(response.fbmsgs(), false));
        ASSERT_EQ(1, fbReader.size());
        ASSERT_TRUE(fbReader.read(0)->merged());
        ASSERT_EQ(2, response.totalmsgcount());
    }
}

void BlockMessageReaderTest::checkFillMessage(const string &lineCount,
                                              const ConsumptionRequest *request,
                                              int expectMsgCount,
                                              ErrorCode ec,
                                              bool compress,
                                              bool merge) {
    prepareFSMessage(_startId, _count, 1, compress, merge);
    FilePairPtr filePair = _fileManager->getFilePairById(_startId);
    assert(filePair);
    int64_t blockSize = _msgCountInBlock * FILE_MESSAGE_META_SIZE; // 8 message in block
    int64_t fileCacheSize = 1280;                                  // 10 block
    PermissionCenter permissionCenter;
    BlockPool fileCachePool(fileCacheSize, blockSize);
    FileCache fileCache(&fileCachePool, 1, 600, &permissionCenter, NULL);
    BlockMessageReader reader(filePair, &fileCache, NULL);
    protocol::MessageResponse response;
    ASSERT_EQ(ec, reader.fillMessage(request, &response)) << lineCount;
    protocol::FBMessageReader fbReader;
    if (response.messageformat() == MF_FB) {
        ASSERT_TRUE(fbReader.init(response.fbmsgs(), false)) << lineCount;
        ASSERT_EQ(expectMsgCount, fbReader.size()) << lineCount;
    } else {
        ASSERT_EQ(expectMsgCount, response.msgs_size()) << lineCount;
    }
    FileMessageMetaVec metaVec;
    int readStartId = request->startid();
    ASSERT_EQ(ERROR_NONE,
              metaVec.init(&fileCache,
                           filePair->metaFileName,
                           false,
                           readStartId - _startId,
                           filePair->getEndMessageId() - _startId,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           true))
        << lineCount;
    char c = 'a' + readStartId - _startId;
    int cursor = 0;
    int64_t actualTotalSize = 0;
    MessageFilter filter(request->filter());
    FileMessageMeta meta;
    for (int i = readStartId - _startId; i < metaVec.end() && cursor < expectMsgCount; ++i) {
        ASSERT_TRUE(metaVec.fillMeta(i, meta)) << lineCount;
        if (filter.filter(meta)) {
            string expectedData((i + 1), c);
            if (compress) {
                autil::ZlibCompressor _compressor(Z_BEST_SPEED);
                _compressor.addDataToBufferIn(expectedData);
                if (_compressor.compress()) {
                    int64_t msgLen = _compressor.getBufferOutLen();
                    expectedData = string(_compressor.getBufferOut(), msgLen);
                }
            }
            actualTotalSize += (i + 1);
            int64_t msgId = i + _startId;
            if (response.messageformat() == MF_PB) {
                ASSERT_EQ(msgId, response.msgs(cursor).msgid()) << lineCount;
                ASSERT_EQ(100 + msgId * 2, response.msgs(cursor).timestamp()) << lineCount;
                ASSERT_EQ(msgId, response.msgs(cursor).uint16payload()) << lineCount;
                ASSERT_EQ(msgId % 256, response.msgs(cursor).uint8maskpayload()) << lineCount;
                ASSERT_EQ(compress, response.msgs(cursor).compress());
                ASSERT_EQ(expectedData, response.msgs(cursor).data()) << lineCount;
            } else {
                const protocol::flat::Message *fbMsg = fbReader.read(cursor);
                ASSERT_EQ(msgId, fbMsg->msgId()) << lineCount;
                ASSERT_EQ(100 + msgId * 2, fbMsg->timestamp()) << lineCount;
                ASSERT_EQ(msgId, fbMsg->uint16Payload()) << lineCount;
                ASSERT_EQ(msgId % 256, fbMsg->uint8MaskPayload()) << lineCount;
                ASSERT_EQ(compress, fbMsg->compress());
                string data((char *)fbMsg->data()->Data(), (int)fbMsg->data()->size());
                ASSERT_EQ(expectedData, data) << lineCount;
            }
            cursor++;
        }
        c++;
    }
    ASSERT_EQ(cursor, expectMsgCount) << lineCount;
}

void BlockMessageReaderTest::prepareFSMessage(
    int64_t startId, int32_t count, uint32_t fileCnt, bool compress, bool merge) {
    fslib::ErrorCode ec;
    ec = fslib::fs::FileSystem::remove(GET_TEMPLATE_DATA_PATH());
    EXPECT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
    ec = fslib::fs::FileSystem::mkDir(GET_TEMPLATE_DATA_PATH(), true);
    EXPECT_TRUE(ec == fslib::EC_OK);

    config::PartitionConfig config;
    config.setCompressMsg(compress);
    config.setDataRoot(GET_TEMPLATE_DATA_PATH());
    config.setMaxCommitTimeAsError(100 * 1000 * 1000);
    DELETE_AND_SET_NULL(_fileManager);
    _fileManager = new FileManager;
    ObsoleteFileCriterion obsoleteFileCriterion;
    ASSERT_EQ(ERROR_NONE, _fileManager->init(GET_TEMPLATE_DATA_PATH(), obsoleteFileCriterion));
    PartitionId partitionId;
    FakeMessageCommitter msgCommitter(partitionId, config, _fileManager, nullptr);
    int64_t start = startId;
    int32_t recordCnt = count / fileCnt;
    int32_t leftCnt = count;
    for (uint32_t i = 0; i < fileCnt; i++) {
        MemoryMessageVector mmv = prepareMemoryMessage(start, recordCnt, compress, merge);
        msgCommitter.write(mmv);
        msgCommitter.commitFile();
        msgCommitter.closeFile();
        start = start + recordCnt;
        leftCnt -= recordCnt;
        if (recordCnt * 2 > leftCnt) {
            recordCnt = leftCnt;
        }
    }
}

MemoryMessageVector
BlockMessageReaderTest::prepareMemoryMessage(int64_t startId, int64_t msgCount, bool compress, bool merge) {
    MemoryMessageVector mmv;
    char c = 'a';
    for (int i = 0; i < msgCount; ++i) {
        MemoryMessage tmp;
        int64_t msgId = startId + i;
        tmp.setMsgId(msgId);
        tmp.setTimestamp(100 + msgId * 2);
        tmp.setPayload(msgId);
        tmp.setMaskPayload(msgId % 256);
        string data((i + 1), c);
        c++;
        if (compress && !merge) {
            autil::ZlibCompressor _compressor(Z_BEST_SPEED);
            _compressor.addDataToBufferIn(data);
            if (_compressor.compress()) {
                int64_t msgLen = _compressor.getBufferOutLen();
                data = string(_compressor.getBufferOut(), msgLen);
            }
            tmp.setCompress(true);
        }

        BlockPtr block(new Block(data.size(), NULL));
        memcpy(block->getBuffer(), data.c_str(), data.size());
        tmp.setBlock(block);
        tmp.setLen(data.size());
        tmp.setOffset(0);
        mmv.push_back(tmp);
    }
    if (merge) {
        MemoryMessageVector msgVec;
        RangeUtil rangeUtil(1, 2);
        MemoryMessageUtil::mergeMemoryMessage(mmv, &rangeUtil, 2, msgVec);
        mmv.clear();
        if (compress && !merge) {
            autil::ZlibCompressor compressor(Z_BEST_SPEED);
            for (size_t i = 0; i < msgVec.size(); i++) {
                compressor.reset();
                util::BlockPtr block = msgVec[i].getBlock();
                compressor.addDataToBufferIn(block->getBuffer(), msgVec[i].getLen());
                if (compressor.compress()) {
                    int64_t msgLen = compressor.getBufferOutLen();
                    memcpy(block->getBuffer(), compressor.getBufferOut(), msgLen);
                    msgVec[i].setLen(msgLen);
                    msgVec[i].setCompress(true);
                }
            }
        }
        return msgVec;
    }
    return mmv;
}

} // namespace storage
} // namespace swift
