#include "swift/broker/storage_dfs/FileMessageMetaVec.h"

#include <iostream>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/storage_dfs/FileCache.h"
#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
namespace swift {
namespace storage {
using namespace swift::util;
using namespace swift::common;
using namespace swift::protocol;

class FileMessageMetaVecTest : public TESTBASE {
public:
    void setUp() {
        fslib::ErrorCode ec = fslib::fs::FileSystem::remove(GET_TEMPLATE_DATA_PATH());
        ASSERT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
        ec = fslib::fs::FileSystem::mkDir(GET_TEMPLATE_DATA_PATH(), true);
        ASSERT_TRUE(ec == fslib::EC_OK);
    }
    bool prepartMeta(int64_t count, const string &fileName) {
        string metaStr;
        for (int64_t i = 0; i < count; i++) {
            FileMessageMeta meta;
            meta.timestamp = i;
            meta.endPos = i + 1;
            metaStr.append((char *)(&meta), FILE_MESSAGE_META_SIZE);
        }
        fslib::ErrorCode ec = fslib::fs::FileSystem::writeFile(fileName, metaStr);
        if (ec != fslib::EC_OK) {
            return false;
        }
        metaStr.clear();
        fslib::fs::FileSystem::readFile(fileName, metaStr);
        ec = fslib::fs::FileSystem::isExist(fileName);
        if (ec != fslib::EC_TRUE) {
            return false;
        }
        return true;
    }
};

TEST_F(FileMessageMetaVecTest, testMetaVec) {
    string fileName = GET_TEMPLATE_DATA_PATH() + "/meta_1";
    int msgCount = 17;
    ASSERT_TRUE(prepartMeta(msgCount, fileName));
    {
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, fileName, false, 0, msgCount, NULL, NULL, NULL, NULL, true);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(0, metaVec.start());
        ASSERT_EQ(4, metaVec.end());
        for (int64_t i = metaVec.start(); i < metaVec.end(); i++) {
            ASSERT_EQ(1, metaVec.getMsgLen(i));
            ASSERT_EQ(i, metaVec.getMsgBegin(i));
        }
        FileMessageMeta meta;
        ASSERT_TRUE(metaVec.fillMeta(2, meta));
        ASSERT_EQ(2, meta.timestamp);
        ASSERT_FALSE(metaVec.fillMeta(4, meta));
        int64_t dfsReadSize, fromCacheSize;
        metaVec.getReadInfo(dfsReadSize, fromCacheSize);
        ASSERT_EQ(64, dfsReadSize);
        ASSERT_EQ(0, fromCacheSize);
    }
    {
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, fileName, false, 8, msgCount, NULL, NULL, NULL, NULL, true);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(8, metaVec.start());
        ASSERT_EQ(12, metaVec.end());
        for (int64_t i = metaVec.start(); i < metaVec.end(); i++) {
            ASSERT_EQ(1, metaVec.getMsgLen(i));
            ASSERT_EQ(i, metaVec.getMsgBegin(i));
        }
        FileMessageMeta meta;
        ASSERT_TRUE(metaVec.fillMeta(8, meta));
        ASSERT_EQ(8, meta.timestamp);
        ASSERT_FALSE(metaVec.fillMeta(7, meta));
        ASSERT_FALSE(metaVec.fillMeta(12, meta));
        int64_t dfsReadSize, fromCacheSize;
        metaVec.getReadInfo(dfsReadSize, fromCacheSize);
        ASSERT_EQ(128, dfsReadSize);
        ASSERT_EQ(0, fromCacheSize);
    }
    {
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, fileName, false, 16, msgCount, NULL, NULL, NULL, NULL, true);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(16, metaVec.start());
        ASSERT_EQ(17, metaVec.end());
        for (int64_t i = metaVec.start(); i < metaVec.end(); i++) {
            ASSERT_EQ(1, metaVec.getMsgLen(i));
            ASSERT_EQ(i, metaVec.getMsgBegin(i));
        }
        FileMessageMeta meta;
        ASSERT_TRUE(metaVec.fillMeta(16, meta));
        ASSERT_EQ(16, meta.timestamp);
        ASSERT_FALSE(metaVec.fillMeta(7, meta));
        ASSERT_FALSE(metaVec.fillMeta(17, meta));
        int64_t dfsReadSize, fromCacheSize;
        metaVec.getReadInfo(dfsReadSize, fromCacheSize);
        ASSERT_EQ(80, dfsReadSize);
        ASSERT_EQ(0, fromCacheSize);
    }
    { // which out message len
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, fileName, false, 16, msgCount, NULL, NULL, NULL, NULL, false);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(16, metaVec.start());
        ASSERT_EQ(17, metaVec.end());
        for (int64_t i = metaVec.start(); i < metaVec.end(); i++) {
            ASSERT_EQ(17, metaVec.getMsgLen(i)); // start offset is 0
            ASSERT_EQ(0, metaVec.getMsgBegin(i));
        }
        FileMessageMeta meta;
        ASSERT_TRUE(metaVec.fillMeta(16, meta));
        ASSERT_EQ(16, meta.timestamp);
        ASSERT_FALSE(metaVec.fillMeta(7, meta));
        ASSERT_FALSE(metaVec.fillMeta(17, meta));
        int64_t dfsReadSize, fromCacheSize;
        metaVec.getReadInfo(dfsReadSize, fromCacheSize);
        ASSERT_EQ(16, dfsReadSize);
        ASSERT_EQ(0, fromCacheSize);
        // from cache
        FileMessageMetaVec metaVec2;
        metaVec2.init(&fileCache, fileName, false, 16, msgCount, NULL, NULL, NULL, NULL, false);
        metaVec2.getReadInfo(dfsReadSize, fromCacheSize);
        ASSERT_EQ(16, dfsReadSize);
        ASSERT_EQ(16, fromCacheSize);
    }
    { // actual message len lager than expect
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, fileName, false, 16, msgCount - 1, NULL, NULL, NULL, NULL, false);
        ASSERT_EQ(ERROR_NONE, ec);
        ASSERT_EQ(16, metaVec.start());
        ASSERT_EQ(16, metaVec.end());
    }
    { // expect message lager than actual
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, fileName, false, 16, msgCount + 1, NULL, NULL, NULL, NULL, false);
        ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
        ASSERT_EQ(16, metaVec.start());
        ASSERT_EQ(17, metaVec.end());
    }
    { // out meta len
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, fileName, false, 20, 20, NULL, NULL, NULL, NULL, false);
        ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    }

    { // file not exist
        BlockPool fileCachePool(192, 64);
        FileCache fileCache(&fileCachePool);
        FileMessageMetaVec metaVec;
        ErrorCode ec = metaVec.init(&fileCache, "/not_exist", false, 16, msgCount, NULL, NULL, NULL, NULL, true);
        ASSERT_EQ(ERROR_BROKER_SOME_MESSAGE_LOST, ec);
    }
}

} // namespace storage
} // namespace swift
