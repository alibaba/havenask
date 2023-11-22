#include "swift/broker/storage_dfs/FileCache.h"

#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/storage_dfs/FileWrapper.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/ReaderRec.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::util;

namespace swift {
namespace storage {

class FileCacheTest : public TESTBASE {
public:
    void setUp() {
        _localTestDataPath = GET_TEMPLATE_DATA_PATH();
        fslib::ErrorCode ec = fslib::fs::FileSystem::remove(_localTestDataPath);
        ASSERT_TRUE(ec == fslib::EC_OK || ec == fslib::EC_NOENT);
        ec = fslib::fs::FileSystem::mkDir(_localTestDataPath, true);
        ASSERT_TRUE(ec == fslib::EC_OK);
    }

    bool prepartData(const string &fileName) {
        string metaStr;
        for (int64_t i = 0; i < 10; i++) {
            metaStr.append(1, 'a' + i);
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

private:
    string _localTestDataPath;
};

TEST_F(FileCacheTest, testGetFileWrapperAppendingChange) {
    string fileName = _localTestDataPath + "/block_data_f_1";
    ASSERT_TRUE(prepartData(fileName));
    BlockPool fileCachePool(9, 3);
    FileCache fileCache(&fileCachePool, 3);
    FileWrapperPtr fileWrapper1 = fileCache.getFileWrapper(fileName, true, 0);
    ASSERT_TRUE(fileWrapper1 != NULL);
    ASSERT_EQ(10, fileWrapper1->getFileLength());
    ASSERT_TRUE(fileWrapper1->isAppending());
    FileWrapperPtr fileWrapper2 = fileCache.getFileWrapper(fileName, false, 10);
    ASSERT_TRUE(fileWrapper2 != NULL);
    ASSERT_TRUE(fileWrapper1.get() == fileWrapper2.get());

    FileWrapperPtr fileWrapper3 = fileCache.getFileWrapper(fileName, false, 11);
    ASSERT_TRUE(fileWrapper3 != NULL);
    ASSERT_TRUE(fileWrapper3.get() != fileWrapper2.get());
    ASSERT_TRUE(!fileWrapper3->isAppending());
    ASSERT_EQ(1, fileCache._fileCache.size());
    ASSERT_EQ(3, fileCache._fileCache[fileName].size());
    ASSERT_TRUE(!fileCache._fileCache[fileName][0]->isAppending());
}

TEST_F(FileCacheTest, testGetFileWrapperAppendingChangeRecycle) {
    string fileName = _localTestDataPath + "/block_data_f_1";
    ASSERT_TRUE(prepartData(fileName));
    BlockPool fileCachePool(9, 3);
    FileCache fileCache(&fileCachePool, 3);
    FileWrapperPtr fileWrapper1 = fileCache.getFileWrapper(fileName, true, 0);
    ASSERT_TRUE(fileWrapper1 != NULL);
    ASSERT_TRUE(fileWrapper1->isAppending());
    ASSERT_EQ(10, fileWrapper1->getFileLength());

    FileWrapperPtr fileWrapper2 = fileCache.getFileWrapper(fileName, false, 10);
    ASSERT_TRUE(fileWrapper2 != NULL);
    ASSERT_TRUE(fileWrapper1.get() == fileWrapper2.get());
    fileCache.recycleFile();
    ASSERT_EQ(0, fileCache._fileCache.size());
}

TEST_F(FileCacheTest, testGetAndRecycleFileWrapperTimeout) {
    string fileName = _localTestDataPath + "/block_data_f_1";
    ASSERT_TRUE(prepartData(fileName));
    string fileName2 = _localTestDataPath + "/block_data_f_2";
    ASSERT_TRUE(prepartData(fileName2));
    string fileName3 = _localTestDataPath + "/block_data_f_3";
    ASSERT_TRUE(prepartData(fileName3));
    BlockPool fileCachePool(9, 3);
    FileCache fileCache(&fileCachePool, 3);
    FileWrapperPtr fileWrapper1 = fileCache.getFileWrapper(fileName, false, 0);
    ASSERT_TRUE(fileWrapper1 != NULL);
    ASSERT_EQ(1, fileCache._fileCache.size());
    ASSERT_EQ(3, fileCache._fileCache[fileName].size());
    FileWrapperPtr fileWrapper2 = fileCache.getFileWrapper(fileName, false, 0);
    ASSERT_TRUE(fileWrapper2 != NULL);
    ASSERT_EQ(1, fileCache._fileCache.size());
    ASSERT_TRUE(fileWrapper1.get() == fileWrapper2.get());
    sleep(3);
    FileWrapperPtr fileWrapper3 = fileCache.getFileWrapper(fileName2, false, 0);
    ASSERT_TRUE(fileWrapper3 != NULL);
    ASSERT_EQ(2, fileCache._fileCache.size());
    ASSERT_EQ(3, fileCache._fileCache[fileName2].size());
    FileWrapperPtr fileWrapper4 = fileCache.getFileWrapper(fileName3, true, 0);
    ASSERT_TRUE(fileWrapper4 != NULL);
    ASSERT_TRUE(fileWrapper4->isAppending());
    ASSERT_EQ(3, fileCache._fileCache.size());
    ASSERT_TRUE(fileCache._fileCache.find(fileName3) != fileCache._fileCache.end());

    sleep(1);
    fileCache.setFileReserveTime(3);
    fileCache.recycleFile();
    ASSERT_EQ(2, fileCache._fileCache.size());
    ASSERT_TRUE(fileCache._fileCache.find(fileName) == fileCache._fileCache.end());
    fileWrapper2 = fileCache.getFileWrapper(fileName, false, 0);
    ASSERT_TRUE(fileWrapper2 != NULL);
    ASSERT_TRUE(fileWrapper1.get() != fileWrapper2.get());
    ASSERT_EQ(3, fileCache._fileCache.size());
    ASSERT_EQ(3, fileCache._fileCache[fileName].size());
}

TEST_F(FileCacheTest, testGetAndRecycleFileWrapperBadFile) {
    string fileName = _localTestDataPath + "/block_data_f_1";
    ASSERT_TRUE(prepartData(fileName));
    string fileName2 = _localTestDataPath + "/block_data_f_2";
    ASSERT_TRUE(prepartData(fileName2));
    string fileName3 = _localTestDataPath + "/block_data_f_3";
    ASSERT_TRUE(prepartData(fileName3));
    BlockPool fileCachePool(9, 3);
    FileCache fileCache(&fileCachePool, 3);
    FileWrapperPtr fileWrapper1 = fileCache.getFileWrapper(fileName, false, 0);
    ASSERT_TRUE(fileWrapper1 != NULL);
    ASSERT_EQ(1, fileCache._fileCache.size());
    ASSERT_EQ(3, fileCache._fileCache[fileName].size());
    FileWrapperPtr fileWrapper2 = fileCache.getFileWrapper(fileName, false, 0);
    ASSERT_TRUE(fileWrapper2 != NULL);
    ASSERT_EQ(1, fileCache._fileCache.size());
    ASSERT_TRUE(fileWrapper1.get() == fileWrapper2.get());
    FileWrapperPtr fileWrapper3 = fileCache.getFileWrapper(fileName2, false, 0);
    ASSERT_TRUE(fileWrapper3 != NULL);
    ASSERT_EQ(2, fileCache._fileCache.size());
    ASSERT_EQ(3, fileCache._fileCache[fileName2].size());
    FileWrapperPtr fileWrapper4 = fileCache.getFileWrapper(fileName3, true, 0);
    ASSERT_TRUE(fileWrapper4 != NULL);
    ASSERT_TRUE(fileWrapper4->isAppending());
    ASSERT_EQ(3, fileCache._fileCache.size());
    ASSERT_TRUE(fileCache._fileCache.find(fileName3) != fileCache._fileCache.end());

    fileWrapper2->setBad(true); // remove bad file
    fileCache.recycleFile();
    ASSERT_EQ(2, fileCache._fileCache.size());
    ASSERT_TRUE(fileCache._fileCache.find(fileName) == fileCache._fileCache.end());

    fileWrapper2 = fileCache.getFileWrapper("/not/exist", false, 0);
    ASSERT_TRUE(fileCache._fileCache.find("/not/exist") != fileCache._fileCache.end());
    ASSERT_TRUE(fileWrapper2 != NULL);
    char buf[10];
    bool succ = false;
    fileWrapper2->pread(buf, 2, 0, succ);
    ASSERT_FALSE(succ);
    ASSERT_EQ(3, fileCache._fileCache.size());
    fileCache.recycleFile();
    ASSERT_EQ(2, fileCache._fileCache.size());
    ASSERT_TRUE(fileCache._fileCache.find("/not/exist") == fileCache._fileCache.end());
}

TEST_F(FileCacheTest, testReadBlcokCacheWithAppendFile) {
    string fileName = _localTestDataPath + "/block_data_10";
    ASSERT_TRUE(prepartData(fileName));
    BlockPool fileCachePool(9, 3);
    FileCache fileCache(&fileCachePool, 1); // total size 9, block size 3
    BlockPtr b1;
    bool isFromCache = false;
    ErrorCode ec = fileCache.readBlock(fileName, 9, true, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(isFromCache);

    ec = fileCache.readBlock(fileName, 9, true, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(isFromCache);

    ec = fileCache.readBlock(fileName, 6, true, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(isFromCache);

    ec = fileCache.readBlock(fileName, 6, true, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_TRUE(isFromCache);
}

TEST_F(FileCacheTest, testReadBlockAndRecycle) {
    string fileName = _localTestDataPath + "/block_data_10";
    ASSERT_TRUE(prepartData(fileName));
    BlockPool fileCachePool(9, 3);
    FileCache fileCache(&fileCachePool); // total size 9, block size 3
    BlockPtr b1;
    bool isFromCache = false;
    ReaderInfoMap readerInfoMap;
    ReaderDes des("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info(new ReaderInfo());
    readerInfoMap[des] = info;

    ErrorCode ec = fileCache.readBlock(fileName, 0, false, NULL, b1, isFromCache, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(isFromCache);

    BlockPtr b2;
    ec = fileCache.readBlock(fileName, 3, false, NULL, b2, isFromCache, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(isFromCache);

    BlockPtr b3;
    ec = fileCache.readBlock(fileName, 6, false, NULL, b3, isFromCache, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(isFromCache);

    BlockPtr b4;
    ec = fileCache.readBlock(fileName, 0, false, NULL, b4, isFromCache, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_TRUE(isFromCache);

    BlockPtr b5; // all block is used
    ec = fileCache.readBlock(fileName, 9, false, NULL, b5, isFromCache, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_BROKER_BUSY, ec);

    b2.reset(); // release block then can recycled in next read

    ec = fileCache.readBlock(fileName, 9, false, NULL, b5, isFromCache, info.get(), &readerInfoMap);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_FALSE(isFromCache);
}

TEST_F(FileCacheTest, testReadBlcokSameBlock) {
    string fileName = _localTestDataPath + "/block_data_10";
    ASSERT_TRUE(prepartData(fileName));
    BlockPool fileCachePool(9, 3);
    FileCache fileCache(&fileCachePool); // total size 9, block size 3
    ErrorCode ec;
    BlockPtr b1;
    bool isFromCache = false;
    ec = fileCache.readBlock(fileName, 0, false, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(3, b1->getActualSize());
    ASSERT_EQ('a', b1->getBuffer()[0]);
    ASSERT_FALSE(isFromCache);

    BlockPtr b2;
    ec = fileCache.readBlock(fileName, 0, false, NULL, b2, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(b1->getBuffer(), b2->getBuffer());
    ASSERT_TRUE(isFromCache);

    BlockPtr b3;
    ec = fileCache.readBlock(fileName, 2, false, NULL, b3, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(b1->getBuffer(), b3->getBuffer());
    ASSERT_TRUE(isFromCache);
}

TEST_F(FileCacheTest, testBackGroudRecycleObsolete) {
    string fileName1 = _localTestDataPath + "/block_data_f_1.data";
    ASSERT_TRUE(prepartData(fileName1));
    string fileName2 = _localTestDataPath + "/block_data_f_2.data";
    ASSERT_TRUE(prepartData(fileName2));
    BlockPool fileCachePool(9, 3); // bufferSize:9, blockSize:3, 共3个block
    FileCache fileCache(&fileCachePool, 3);
    BlockPtr b1;
    bool isFromCache;
    ErrorCode ec = fileCache.readBlock(fileName1, 0, false, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ec = fileCache.readBlock(fileName2, 0, false, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ec = fileCache.readBlock(fileName2, 3, false, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(2, fileCache._fileCache.size());
    ASSERT_EQ(0, fileCache._metaBlockCache.size());
    ASSERT_EQ(3, fileCache._dataBlockCache.size());
    fileCache.setFileReserveTime(3);
    sleep(4);
    ReaderInfoMap readerInfoMap;
    ReaderDes des("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info(new ReaderInfo());
    info->dataInfo->fileName = fileName1;
    info->dataInfo->blockId = 0;
    info->setReadFile(true);
    readerInfoMap[des] = info;
    fileCache.recycle(&readerInfoMap, -1, -1);
    ASSERT_EQ(3, fileCache._dataBlockCache.size());

    info->dataInfo->fileName = fileName2;
    info->dataInfo->blockId = 0;
    fileCache.recycle(&readerInfoMap, -1, -1);
    ASSERT_EQ(2, fileCache._dataBlockCache.size());

    info->dataInfo->fileName = fileName2;
    info->dataInfo->blockId = 1;
    fileCache.recycle(&readerInfoMap, 1, 1);
    ASSERT_EQ(1, fileCache._dataBlockCache.size());

    info->dataInfo->fileName = fileName2;
    info->dataInfo->blockId = 2;
    fileCache.recycle(&readerInfoMap, 1, 1);
    ASSERT_EQ(0, fileCache._dataBlockCache.size());
}

TEST_F(FileCacheTest, testBackGroudRecycleDis) {
    string fileName1 = _localTestDataPath + "/2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.data";
    ASSERT_TRUE(prepartData(fileName1));
    string fileName2 = _localTestDataPath + "/2020-05-11-16-18-10.000000.0000000000003000.1589023243000000.data";
    ASSERT_TRUE(prepartData(fileName2));
    cout << "xxx path: " << _localTestDataPath << endl << fileName1 << endl << fileName2 << endl;
    BlockPool fileCachePool(9, 3); // bufferSize:9, blockSize:3, 共3个block
    FileCache fileCache(&fileCachePool, 3);
    BlockPtr b1;
    bool isFromCache;
    ErrorCode ec = fileCache.readBlock(fileName1, 0, false, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ec = fileCache.readBlock(fileName2, 0, false, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ec = fileCache.readBlock(fileName2, 3, false, NULL, b1, isFromCache);
    ASSERT_EQ(ERROR_NONE, ec);
    ASSERT_EQ(2, fileCache._fileCache.size());
    ASSERT_EQ(0, fileCache._metaBlockCache.size());
    ASSERT_EQ(3, fileCache._dataBlockCache.size());

    ReaderInfoMap readerInfoMap;
    ReaderDes des("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info(new ReaderInfo());
    info->dataInfo->fileName = fileName1;
    info->dataInfo->blockId = 0;
    info->setReadFile(true);
    info->dataInfo->msgSize = 100;
    readerInfoMap[des] = info;
    fileCache.recycle(&readerInfoMap, 1, 1);
    ASSERT_EQ(3, fileCache._dataBlockCache.size());
    vector<int64_t> metaDisVec, dataDisVec;
    info->dataInfo->_latedData.push_back(make_pair(autil::TimeUtility::currentTime(), 10));
    fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);
    fileCache.recycle(&readerInfoMap, 1, 1);
    ASSERT_EQ(1, fileCache._dataBlockCache.size());
    fileCache.recycle(&readerInfoMap, 0, 0);
    ASSERT_EQ(0, fileCache._dataBlockCache.size());
}

TEST_F(FileCacheTest, testGetNearestReaderDis) {
    const int64_t INT64_MAX_VALUE = numeric_limits<int64_t>::max();
    BlockPool fileCachePool(100, 10); // bufferSize:100, blockSize:10字节, 共10个block
    FileCache fileCache(&fileCachePool);
    string metaFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.meta");
    string metaFile2("2020-05-11-17-00-10.000000.0000000000002000.1589023245000000.meta");
    string dataFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.data");
    string dataFile2("2020-05-11-17-00-10.000000.0000000000002000.1589023245000000.data");

    { // 1. no reader info
        ReaderInfoMap readerInfoMap;
        FileCache::FileBlockKey blockKey = make_pair(dataFile1, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
    }
    { // 2. one reader no rate
        ReaderInfoMap readerInfoMap;
        FileCache::FileBlockKey blockKey;
        ReaderDes des("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info(new ReaderInfo());
        info->setReadFile(true);
        info->metaInfo->fileName = metaFile1;
        info->metaInfo->blockId = 1;
        info->dataInfo->fileName = dataFile1;
        info->dataInfo->blockId = 1;
        readerInfoMap[des] = info;

        // meta dis
        blockKey = make_pair(metaFile1, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 1);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 2);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile2, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));

        // data dis
        blockKey = make_pair(dataFile1, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 1);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 2);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile2, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
    }
    { // 2. normal, one reader has rate
        ReaderInfoMap readerInfoMap;
        FileCache::FileBlockKey blockKey;
        ReaderDes des("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info(new ReaderInfo());
        info->setReadFile(true);
        info->metaInfo->fileName = metaFile1;
        info->metaInfo->blockId = 1;
        info->metaInfo->updateRate(5); //每秒读5字节
        info->metaInfo->msgSize = 16;
        info->dataInfo->fileName = dataFile1;
        info->dataInfo->blockId = 1;
        info->dataInfo->updateRate(10); //每秒读10字节
        info->dataInfo->msgSize = 64;
        readerInfoMap[des] = info;

        // meta dis
        blockKey = make_pair(metaFile1, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 1);
        ASSERT_EQ(0, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 2);
        ASSERT_EQ(2, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 3);
        ASSERT_EQ(4, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile2, 0);
        ASSERT_EQ(3198, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));

        // data dis
        blockKey = make_pair(dataFile1, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 1);
        ASSERT_EQ(0, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 2);
        ASSERT_EQ(1, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 3);
        ASSERT_EQ(2, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile2, 0);
        ASSERT_EQ(6399, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
    }
    { // 2. normal, two readers has rate
        ReaderInfoMap readerInfoMap;
        FileCache::FileBlockKey blockKey;
        ReaderDes des("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info(new ReaderInfo());
        info->setReadFile(true);
        info->metaInfo->fileName = metaFile1;
        info->metaInfo->blockId = 4;
        info->metaInfo->updateRate(2); //每秒读2字节
        info->dataInfo->fileName = dataFile1;
        info->dataInfo->blockId = 1;
        info->dataInfo->updateRate(5); //每秒读5字节
        info->dataInfo->msgSize = 16;
        readerInfoMap[des] = info;

        ReaderDes des2("10.1.1.1:100", 0, 30);
        ReaderInfoPtr info2(new ReaderInfo());
        info2->setReadFile(true);
        info2->metaInfo->fileName = metaFile1;
        info2->metaInfo->blockId = 1;
        info2->metaInfo->updateRate(5); //每秒读5字节
        info2->dataInfo->fileName = dataFile2;
        info2->dataInfo->blockId = 1;
        info2->dataInfo->updateRate(10); //每秒读10字节
        info2->dataInfo->msgSize = 32;
        readerInfoMap[des2] = info2;

        // meta dis
        blockKey = make_pair(metaFile1, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 1);
        ASSERT_EQ(0, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 2);
        ASSERT_EQ(2, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 3);
        ASSERT_EQ(4, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 4);
        ASSERT_EQ(0, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 5);
        ASSERT_EQ(5, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 6);
        ASSERT_EQ(10, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile1, 7);
        ASSERT_EQ(12, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile2, 0);
        ASSERT_EQ(3198, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));
        blockKey = make_pair(metaFile2, 10);
        ASSERT_EQ(3218, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, false));

        // data dis
        blockKey = make_pair(dataFile1, 0);
        ASSERT_EQ(INT64_MAX_VALUE, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 1);
        ASSERT_EQ(0, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 2);
        ASSERT_EQ(2, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile1, 11);
        ASSERT_EQ(20, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile2, 0);
        ASSERT_EQ(3198, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile2, 1);
        ASSERT_EQ(0, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile2, 2);
        ASSERT_EQ(1, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
        blockKey = make_pair(dataFile2, 11);
        ASSERT_EQ(10, fileCache.getNearestReaderDis(blockKey, &readerInfoMap, true));
    }
}

TEST_F(FileCacheTest, testGetBlockDis) {
    const int64_t INT64_MAX_VALUE = numeric_limits<int64_t>::max();
    BlockPool fileCachePool(100, 10); // bufferSize:100, blockSize:10字节, 共10个block
    FileCache fileCache(&fileCachePool);
    string metaFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.meta");
    string metaFile2("2020-05-11-16-18-10.000000.0000000000003000.1589023249000000.meta");
    string dataFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.data");
    string dataFile2("2020-05-11-16-18-10.000000.0000000000003000.1589023249000000.data");

    // 1. no reader no block
    ReaderInfoMap readerInfoMap;
    vector<int64_t> metaDisVec;
    vector<int64_t> dataDisVec;
    fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);
    ASSERT_EQ(0, metaDisVec.size());
    ASSERT_EQ(0, dataDisVec.size());

    // 2. no reader has block
    FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
    FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
    fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
    fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);
    ASSERT_EQ(1, metaDisVec.size());
    ASSERT_EQ(1, dataDisVec.size());
    ASSERT_EQ(INT64_MAX_VALUE, metaDisVec[0]);
    ASSERT_EQ(INT64_MAX_VALUE, dataDisVec[0]);

    // 3. has one reader and one block
    ReaderDes des("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info(new ReaderInfo());
    info->setReadFile(true);
    info->metaInfo->fileName = metaFile1;
    info->metaInfo->blockId = 1;
    info->metaInfo->updateRate(5); //每秒读5字节
    info->dataInfo->fileName = dataFile1;
    info->dataInfo->blockId = 1;
    info->dataInfo->updateRate(10); //每秒读10字节
    readerInfoMap[des] = info;      //没设msgSize，data跨文件距离都是max
    fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);
    ASSERT_EQ(2, metaDisVec.size()); //累加
    ASSERT_EQ(2, dataDisVec.size()); //累加
    ASSERT_EQ(INT64_MAX_VALUE, metaDisVec[0]);
    ASSERT_EQ(INT64_MAX_VALUE, dataDisVec[0]);
    ASSERT_EQ(0, metaDisVec[1]);
    ASSERT_EQ(0, dataDisVec[1]);

    // 4. more blocks
    metaDisVec.clear(); // clear first
    dataDisVec.clear(); // clear first
    fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
    fileCache._metaBlockCache[make_pair(metaFile1, 2)] = metaBlock;
    fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 2)] = dataBlock;
    fileCache._dataBlockCache[make_pair(dataFile2, 0)] = dataBlock;
    fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);
    ASSERT_EQ(4, metaDisVec.size());
    ASSERT_EQ(4, dataDisVec.size());
    ASSERT_EQ(INT64_MAX_VALUE, metaDisVec[0]);
    ASSERT_EQ(0, metaDisVec[1]);
    ASSERT_EQ(2, metaDisVec[2]);
    ASSERT_EQ(6398, metaDisVec[3]);
    ASSERT_EQ(INT64_MAX_VALUE, dataDisVec[0]);
    ASSERT_EQ(0, dataDisVec[1]);
    ASSERT_EQ(1, dataDisVec[2]);
    ASSERT_EQ(INT64_MAX_VALUE, dataDisVec[3]);

    // 5. more readers, two for example
    readerInfoMap.clear();
    ReaderDes des1("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info1(new ReaderInfo());
    info1->setReadFile(true);
    info1->metaInfo->fileName = metaFile1;
    info1->metaInfo->blockId = 1;
    info1->metaInfo->updateRate(2); //每秒读2字节
    info1->dataInfo->fileName = dataFile1;
    info1->dataInfo->blockId = 1;
    info1->dataInfo->updateRate(5); //每秒读5字节
    info1->dataInfo->msgSize = 100; //消息大小100字节
    readerInfoMap[des1] = info1;

    ReaderDes des2("10.1.1.1:100", 0, 30);
    ReaderInfoPtr info2(new ReaderInfo());
    info2->setReadFile(true);
    info2->metaInfo->fileName = metaFile1;
    info2->metaInfo->blockId = 4;
    info2->metaInfo->updateRate(5); //每秒读5字节
    info2->dataInfo->fileName = dataFile2;
    info2->dataInfo->blockId = 1;
    info2->dataInfo->updateRate(10); //每秒读10字节
    info2->dataInfo->msgSize = 100;  //消息大小100字节，可计算跨文件距离
    readerInfoMap[des2] = info2;

    fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);
    ASSERT_EQ(8, metaDisVec.size()); //前4个是上次的值
    ASSERT_EQ(8, dataDisVec.size()); //前4个是上次的值
    ASSERT_EQ(INT64_MAX_VALUE, metaDisVec[0]);
    ASSERT_EQ(0, metaDisVec[1]);
    ASSERT_EQ(2, metaDisVec[2]);
    ASSERT_EQ(6398, metaDisVec[3]);
    ASSERT_EQ(INT64_MAX_VALUE, metaDisVec[4]);
    ASSERT_EQ(0, metaDisVec[5]);
    ASSERT_EQ(5, metaDisVec[6]);
    ASSERT_EQ(6392, metaDisVec[7]);

    ASSERT_EQ(INT64_MAX_VALUE, dataDisVec[0]);
    ASSERT_EQ(0, dataDisVec[1]);
    ASSERT_EQ(1, dataDisVec[2]);
    ASSERT_EQ(INT64_MAX_VALUE, dataDisVec[3]);
    ASSERT_EQ(INT64_MAX_VALUE, dataDisVec[4]);
    ASSERT_EQ(0, dataDisVec[5]);
    ASSERT_EQ(2, dataDisVec[6]);
    ASSERT_EQ(39998, dataDisVec[7]);
}

TEST_F(FileCacheTest, testRecycleBlockObsolete) {
    BlockPool fileCachePool(100, 10); // bufferSize:100, blockSize:10字节, 共10个block
    FileCache fileCache(&fileCachePool);
    string metaFile1("11.meta");
    string metaFile2("22.meta");
    string dataFile1("11.data");
    string dataFile2("22.data");
    monitor::RecycleBlockMetricsCollector collector("topic", 0);
    { // 1. no reader no blocks
        ReaderInfoMap readerInfoMap;
        ASSERT_EQ(0, fileCache.recycleBlockObsolete(&readerInfoMap, collector));
    }
    { // 2. no reader has blocks, recycle all
        FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
        FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
        fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile2, 0)] = dataBlock;
        ReaderInfoMap readerInfoMap;
        ASSERT_EQ(6, fileCache.recycleBlockObsolete(&readerInfoMap, collector));
    }
    { // 3. one reader has blocks
        FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
        FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
        fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile2, 0)] = dataBlock;

        ReaderInfoMap readerInfoMap;
        ReaderDes des1("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info1(new ReaderInfo());
        info1->setReadFile(true);
        info1->metaInfo->fileName = metaFile1;
        info1->metaInfo->blockId = 1;
        info1->metaInfo->updateRate(2); //每秒读2字节
        info1->dataInfo->fileName = dataFile1;
        info1->dataInfo->blockId = 1;
        info1->dataInfo->updateRate(5); //每秒读5字节
        readerInfoMap[des1] = info1;
        // will recycle metaFile1 block 0 and dataFile1 block 0
        ASSERT_EQ(2, fileCache.recycleBlockObsolete(&readerInfoMap, collector));
    }
    { // 4. two readers have blocks
        FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
        FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
        fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile2, 0)] = dataBlock;

        ReaderInfoMap readerInfoMap;
        ReaderDes des1("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info1(new ReaderInfo());
        info1->setReadFile(true);
        info1->metaInfo->fileName = metaFile1;
        info1->metaInfo->blockId = 1;
        info1->metaInfo->updateRate(2); //每秒读2字节
        info1->dataInfo->fileName = dataFile1;
        info1->dataInfo->blockId = 1;
        info1->dataInfo->updateRate(5); //每秒读5字节
        readerInfoMap[des1] = info1;

        ReaderDes des2("10.1.1.1:100", 0, 30);
        ReaderInfoPtr info2(new ReaderInfo());
        info2->setReadFile(true);
        info2->metaInfo->fileName = metaFile1;
        info2->metaInfo->blockId = 4;
        info2->metaInfo->updateRate(5); //每秒读5字节
        info2->dataInfo->fileName = dataFile2;
        info2->dataInfo->blockId = 1;
        info2->dataInfo->updateRate(10); //每秒读10字节
        readerInfoMap[des2] = info2;
        // will recycle metaFile1 block 0 and dataFile1 block 0
        ASSERT_EQ(2, fileCache.recycleBlockObsolete(&readerInfoMap, collector));
    }
}

TEST_F(FileCacheTest, testRecycleBlockBysDis) {
    BlockPool fileCachePool(100, 10); // bufferSize:100, blockSize:10字节, 共10个block
    FileCache fileCache(&fileCachePool);
    string metaFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.meta");
    string metaFile2("2020-05-11-17-00-10.000000.0000000000002000.1589023245000000.meta");
    string dataFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.data");
    string dataFile2("2020-05-11-17-00-10.000000.0000000000002000.1589023245000000.data");
    monitor::RecycleBlockMetricsCollector collector("topic", 0);
    { // 1. no reader no blocks
        ReaderInfoMap readerInfoMap;
        ASSERT_EQ(0, fileCache.recycleBlockByDis(&readerInfoMap, 4, 4, collector));
    }
    { // 2. no reader has blocks, recycle all
        FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
        FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
        fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile2, 0)] = dataBlock;
        ReaderInfoMap readerInfoMap;
        vector<int64_t> metaDisVec;
        vector<int64_t> dataDisVec;
        fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);
        ASSERT_EQ(0, fileCache.recycleBlockByDis(&readerInfoMap, -1, -1, collector));
        ASSERT_EQ(6, fileCache.recycleBlockByDis(&readerInfoMap, 4, 4, collector));
    }
    { // 3. one reader has blocks
        FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
        FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
        fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 2)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 2)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile2, 0)] = dataBlock;

        ReaderInfoMap readerInfoMap;
        ReaderDes des1("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info1(new ReaderInfo());
        info1->setReadFile(true);
        info1->metaInfo->fileName = metaFile1;
        info1->metaInfo->blockId = 1;
        info1->metaInfo->updateRate(2); //每秒读2字节
        info1->dataInfo->fileName = dataFile1;
        info1->dataInfo->blockId = 1;
        info1->dataInfo->updateRate(5); //每秒读5字节
        info1->dataInfo->msgSize = 10;  //消息大小10字节
        readerInfoMap[des1] = info1;

        vector<int64_t> metaDisVec;
        vector<int64_t> dataDisVec;
        fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);

        // will recycle m1_0, m1_2, m2_0; d1_0, d2_0
        ASSERT_EQ(5, fileCache.recycleBlockByDis(&readerInfoMap, 4, 4, collector));
        // will recycle d1_2
        ASSERT_EQ(1, fileCache.recycleBlockByDis(&readerInfoMap, 4, 1, collector));
    }
    { // 4. two readers have blocks
        FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
        FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
        fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 2)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 3)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 4)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 5)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 6)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile1, 7)] = metaBlock;
        fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile1, 10)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile2, 0)] = dataBlock;
        fileCache._dataBlockCache[make_pair(dataFile2, 1)] = dataBlock;

        ReaderInfoMap readerInfoMap;
        ReaderDes des1("10.1.1.1:100", 0, 20);
        ReaderInfoPtr info1(new ReaderInfo());
        info1->setReadFile(true);
        info1->metaInfo->fileName = metaFile1;
        info1->metaInfo->blockId = 1;
        info1->metaInfo->updateRate(2); //每秒读2字节
        info1->dataInfo->fileName = dataFile1;
        info1->dataInfo->blockId = 1;
        info1->dataInfo->updateRate(5); //每秒读5字节
        info1->dataInfo->msgSize = 10;  //消息大小10字节
        readerInfoMap[des1] = info1;

        ReaderDes des2("10.1.1.1:100", 0, 30);
        ReaderInfoPtr info2(new ReaderInfo());
        info2->setReadFile(true);
        info2->metaInfo->fileName = metaFile1;
        info2->metaInfo->blockId = 4;
        info2->metaInfo->updateRate(5); //每秒读5字节
        info2->dataInfo->fileName = dataFile2;
        info2->dataInfo->blockId = 1;
        info2->dataInfo->updateRate(10); //每秒读10字节
        info2->dataInfo->msgSize = 10;   //消息大小10字节
        readerInfoMap[des2] = info2;

        vector<int64_t> metaDisVec;
        vector<int64_t> dataDisVec;
        fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);

        // will recycle m1_0, m1_2, m1_3, m1_6, m1_7, m2_0; d1_0, d1_10, d2_0
        ASSERT_EQ(9, fileCache.recycleBlockByDis(&readerInfoMap, 4, 4, collector));
    }
}

TEST_F(FileCacheTest, testRecycleBlock1) {
    BlockPool fileCachePool(100, 10); // bufferSize:100, blockSize:10字节, 共10个block
    FileCache fileCache(&fileCachePool);
    string metaFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.meta");
    string metaFile2("2020-05-11-17-00-10.000000.0000000000002000.1589023245000000.meta");
    string dataFile1("2020-05-11-16-18-10.000000.0000000000001000.1589023243000000.data");
    string dataFile2("2020-05-11-17-00-10.000000.0000000000002000.1589023245000000.data");

    // two readers have blocks
    FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
    FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
    fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
    fileCache._metaBlockCache[make_pair(metaFile1, 1)] = metaBlock;
    fileCache._metaBlockCache[make_pair(metaFile1, 2)] = metaBlock;
    fileCache._metaBlockCache[make_pair(metaFile1, 3)] = metaBlock;
    fileCache._metaBlockCache[make_pair(metaFile2, 0)] = metaBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 2)] = dataBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 3)] = dataBlock;
    fileCache._dataBlockCache[make_pair(dataFile2, 1)] = dataBlock;
    ASSERT_EQ(5, fileCache._metaBlockCache.size());
    ASSERT_EQ(5, fileCache._dataBlockCache.size());

    ReaderInfoMap readerInfoMap;
    ReaderDes des1("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info1(new ReaderInfo());
    info1->setReadFile(true);
    info1->metaInfo->fileName = metaFile1;
    info1->metaInfo->blockId = 1;
    info1->metaInfo->updateRate(2); //每秒读2字节
    info1->dataInfo->fileName = dataFile1;
    info1->dataInfo->blockId = 0;
    info1->dataInfo->updateRate(5); //每秒读5字节
    info1->dataInfo->msgSize = 10;
    readerInfoMap[des1] = info1;

    ReaderDes des2("10.1.1.1:100", 0, 30);
    ReaderInfoPtr info2(new ReaderInfo());
    info2->setReadFile(true);
    info2->metaInfo->fileName = metaFile1;
    info2->metaInfo->blockId = 3;
    info2->metaInfo->updateRate(5); //每秒读5字节
    info2->dataInfo->fileName = dataFile2;
    info2->dataInfo->blockId = 1;
    info2->dataInfo->updateRate(10); //每秒读10字节
    info2->dataInfo->msgSize = 10;
    readerInfoMap[des2] = info2;

    vector<int64_t> metaDisVec;
    vector<int64_t> dataDisVec;
    fileCache.getBlockDis(&readerInfoMap, metaDisVec, dataDisVec);

    fileCachePool._usedBlockCount = 5;
    fileCache.recycleBlock(1, &readerInfoMap, 5, 5); //有空余，直接return
    ASSERT_EQ(5, fileCache._metaBlockCache.size());
    ASSERT_EQ(5, fileCache._dataBlockCache.size());

    // obsolete回收大于10%, 不做dis回收,  m1_0
    fileCache.recycleBlock(0, &readerInfoMap, 5, 5);
    ASSERT_EQ(4, fileCache._metaBlockCache.size());
    ASSERT_EQ(5, fileCache._dataBlockCache.size());

    // obsolete回收小于10%, 但阈值为-1, 不做dis回收
    fileCache.recycleBlock(0, &readerInfoMap, -1, -1);
    ASSERT_EQ(4, fileCache._metaBlockCache.size());
    ASSERT_EQ(5, fileCache._dataBlockCache.size());

    // obsolete回收小于10%, 做dis回收,  m1_2, m2_0, d1_2, d1_3
    fileCache.recycleBlock(0, &readerInfoMap, 5, 4);
    ASSERT_EQ(2, fileCache._metaBlockCache.size());
    ASSERT_EQ(3, fileCache._dataBlockCache.size());
    fileCachePool._usedBlockCount = 0;
}

TEST_F(FileCacheTest, testRecycleBlockOccupyed) {
    BlockPool fileCachePool(60, 10); // bufferSize:60, blockSize:10字节, 共6个block
    FileCache fileCache(&fileCachePool);
    string metaFile1("11.meta");
    string dataFile1("22.data");

    // 3 readers踩中3个不同block
    fileCache._metaBlockCache[make_pair(metaFile1, 0)] = make_pair(fileCachePool.allocate(), 0);
    fileCache._metaBlockCache[make_pair(metaFile1, 1)] = make_pair(fileCachePool.allocate(), 0);
    fileCache._metaBlockCache[make_pair(metaFile1, 2)] = make_pair(fileCachePool.allocate(), 0);
    fileCache._dataBlockCache[make_pair(dataFile1, 0)] = make_pair(fileCachePool.allocate(), 0);
    fileCache._dataBlockCache[make_pair(dataFile1, 1)] = make_pair(fileCachePool.allocate(), 0);
    fileCache._dataBlockCache[make_pair(dataFile1, 2)] = make_pair(fileCachePool.allocate(), 0);
    ASSERT_EQ(3, fileCache._metaBlockCache.size());
    ASSERT_EQ(3, fileCache._dataBlockCache.size());

    ReaderInfoMap readerInfoMap;
    ReaderDes des1("10.1.1.1:100", 0, 20);
    ReaderInfoPtr info1(new ReaderInfo());
    info1->setReadFile(true);
    info1->metaInfo->fileName = metaFile1;
    info1->metaInfo->blockId = 0;
    info1->metaInfo->updateRate(2); //每秒读2字节
    info1->dataInfo->fileName = dataFile1;
    info1->dataInfo->blockId = 0;
    info1->dataInfo->updateRate(5); //每秒读5字节
    readerInfoMap[des1] = info1;

    ReaderDes des2("10.1.1.1:100", 0, 30);
    ReaderInfoPtr info2(new ReaderInfo());
    info2->setReadFile(true);
    info2->metaInfo->fileName = metaFile1;
    info2->metaInfo->blockId = 1;
    info2->metaInfo->updateRate(3);
    info2->dataInfo->fileName = dataFile1;
    info2->dataInfo->blockId = 1;
    info2->dataInfo->updateRate(6);
    readerInfoMap[des2] = info2;

    ReaderDes des3("10.1.1.1:100", 0, 40);
    ReaderInfoPtr info3(new ReaderInfo());
    info3->setReadFile(true);
    info3->metaInfo->fileName = metaFile1;
    info3->metaInfo->blockId = 2;
    info3->metaInfo->updateRate(5);
    info3->dataInfo->fileName = dataFile1;
    info3->dataInfo->blockId = 2;
    info3->dataInfo->updateRate(10); //每秒读10字节
    readerInfoMap[des3] = info3;

    fileCache.recycleBlock(1, &readerInfoMap, 5, 5);
    ASSERT_EQ(3, fileCache._metaBlockCache.size());
    ASSERT_EQ(3, fileCache._dataBlockCache.size());

    //距离太长回收不到
    fileCache.recycleBlock(0, &readerInfoMap, 5, 5);
    ASSERT_EQ(3, fileCache._metaBlockCache.size());
    ASSERT_EQ(3, fileCache._dataBlockCache.size());

    //距离太短，不做dis
    fileCache.recycleBlock(0, &readerInfoMap, -1, -1);
    ASSERT_EQ(3, fileCache._metaBlockCache.size());
    ASSERT_EQ(3, fileCache._dataBlockCache.size());

    //距离为0，回收所有
    fileCache.recycleBlock(0, &readerInfoMap, 0, 0);
    ASSERT_EQ(0, fileCache._metaBlockCache.size());
    ASSERT_EQ(0, fileCache._dataBlockCache.size());
}

TEST_F(FileCacheTest, testGetBlockFromCache) {
    BlockPool fileCachePool(100, 10); // bufferSize:100, blockSize:10字节, 共10个block
    FileCache fileCache(&fileCachePool);
    string metaFile1("11.meta");
    string metaFile2("22.meta");
    string dataFile1("11.data");
    string dataFile2("22.data");

    FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(new Block(10)), 0);
    FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(new Block(10)), 0);
    fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(metaFile1, 0, &info);
        ASSERT_TRUE(block != nullptr);
        ASSERT_EQ(metaFile1, info.metaInfo->fileName);
        ASSERT_EQ(0, info.metaInfo->blockId);
        ASSERT_TRUE(info.dataInfo->fileName.empty());
        ASSERT_EQ(-1, info.dataInfo->blockId);
    }
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(metaFile1, 9, &info);
        ASSERT_TRUE(block != nullptr);
        ASSERT_EQ(metaFile1, info.metaInfo->fileName);
        ASSERT_EQ(0, info.metaInfo->blockId);
        ASSERT_TRUE(info.dataInfo->fileName.empty());
        ASSERT_EQ(-1, info.dataInfo->blockId);
    }
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(metaFile1, 10, &info);
        ASSERT_TRUE(block == nullptr);
        ASSERT_EQ(metaFile1, info.metaInfo->fileName);
        ASSERT_EQ(1, info.metaInfo->blockId);
        ASSERT_TRUE(info.dataInfo->fileName.empty());
        ASSERT_EQ(-1, info.dataInfo->blockId);
    }
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(metaFile2, 0, &info);
        ASSERT_TRUE(block == nullptr);
        ASSERT_EQ(metaFile2, info.metaInfo->fileName);
        ASSERT_EQ(0, info.metaInfo->blockId);
        ASSERT_TRUE(info.dataInfo->fileName.empty());
        ASSERT_EQ(-1, info.dataInfo->blockId);
    }
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(dataFile1, 0, &info);
        ASSERT_TRUE(block != nullptr);
        ASSERT_EQ(dataFile1, info.dataInfo->fileName);
        ASSERT_EQ(0, info.dataInfo->blockId);
        ASSERT_TRUE(info.metaInfo->fileName.empty());
        ASSERT_EQ(-1, info.metaInfo->blockId);
    }
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(dataFile1, 9, &info);
        ASSERT_TRUE(block != nullptr);
        ASSERT_EQ(dataFile1, info.dataInfo->fileName);
        ASSERT_EQ(0, info.dataInfo->blockId);
        ASSERT_TRUE(info.metaInfo->fileName.empty());
        ASSERT_EQ(-1, info.metaInfo->blockId);
    }
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(dataFile1, 10, &info);
        ASSERT_TRUE(block == nullptr);
        ASSERT_EQ(dataFile1, info.dataInfo->fileName);
        ASSERT_EQ(1, info.dataInfo->blockId);
        ASSERT_TRUE(info.metaInfo->fileName.empty());
        ASSERT_EQ(-1, info.metaInfo->blockId);
    }
    {
        ReaderInfo info;
        BlockPtr block;
        block = fileCache.getBlockFromCache(dataFile2, 0, &info);
        ASSERT_TRUE(block == nullptr);
        ASSERT_EQ(dataFile2, info.dataInfo->fileName);
        ASSERT_EQ(0, info.dataInfo->blockId);
        ASSERT_TRUE(info.metaInfo->fileName.empty());
        ASSERT_EQ(-1, info.metaInfo->blockId);
    }
}

TEST_F(FileCacheTest, testGetCount) {
    BlockPool fileCachePool(100, 10);
    FileCache fileCache(&fileCachePool);

    ASSERT_EQ(0, fileCache.getCacheMetaBlockCount());
    ASSERT_EQ(0, fileCache.getCacheDataBlockCount());

    string metaFile1("11.meta");
    string dataFile1("11.data");
    FileCache::FileBlockItem metaBlock = make_pair(BlockPtr(), 0);
    FileCache::FileBlockItem dataBlock = make_pair(BlockPtr(), 0);
    fileCache._metaBlockCache[make_pair(metaFile1, 0)] = metaBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 0)] = dataBlock;
    fileCache._dataBlockCache[make_pair(dataFile1, 1)] = dataBlock;
    ASSERT_EQ(1, fileCache.getCacheMetaBlockCount());
    ASSERT_EQ(2, fileCache.getCacheDataBlockCount());
}

TEST_F(FileCacheTest, testGetFileMsgId) {
    string fileName = "2020-05-10-21-07-27.517054.0000024160491391.1589116047517054.meta";
    BlockPool fileCachePool(100, 10);
    FileCache fileCache(&fileCachePool);
    int64_t msgId = -1;
    ASSERT_TRUE(fileCache.getFileMsgId(fileName, msgId));
    ASSERT_EQ(24160491391, msgId);

    fileName = "2020-05-10-21-07-27.517054.0000024160491391.meta";
    ASSERT_TRUE(fileCache.getFileMsgId(fileName, msgId));
    ASSERT_EQ(24160491391, msgId);

    fileName = "2020-05-10-21-07-27.517054.0000000000000000.meta";
    ASSERT_TRUE(fileCache.getFileMsgId(fileName, msgId));
    ASSERT_EQ(0, msgId);

    fileName = "2020-05-10-21-07-27.517054.999999999999999.meta";
    ASSERT_TRUE(fileCache.getFileMsgId(fileName, msgId));
    ASSERT_EQ(999999999999999, msgId);

    fileName = "2020-05-10-21-07-27.517054.999999999999999.1580154545.meta";
    ASSERT_TRUE(fileCache.getFileMsgId(fileName, msgId));
    ASSERT_EQ(999999999999999, msgId);

    fileName = "/swift/storage_dfs/test/test.runfiles/com_taobao_aios//"
               "2020-05-10-21-07-27.517054.999999999999999.1580154545.meta";
    ASSERT_TRUE(fileCache.getFileMsgId(fileName, msgId));
    ASSERT_EQ(999999999999999, msgId);

    fileName = "/swift/storage_dfs/test/runfiles/com_taobao_aios//"
               "2020-05-10-21-07-27.517054.999999999999999.1580154545.meta";
    ASSERT_TRUE(fileCache.getFileMsgId(fileName, msgId));
    ASSERT_EQ(999999999999999, msgId);
}

TEST_F(FileCacheTest, testCollectBlockMetrics) {
    BlockPtr block(new Block(100));
    block->_actualBufferSize = 10;
    FileCache fc(nullptr);
    { // 1. read data from cache
        monitor::ReadMetricsCollector collector("topic", 0);
        fc.collectBlockMetrics(ERROR_NONE, "file.data", true, block, &collector);
        EXPECT_EQ(1, collector.partitionReadBlockQps);
        EXPECT_EQ(1, collector.partitionBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadRate);
        EXPECT_EQ(0, collector.actualReadBlockCount);
        EXPECT_EQ(1, collector.partitionReadDataBlockQps);
        EXPECT_EQ(0, collector.partitionReadDataBlockErrorQps);
        EXPECT_EQ(1, collector.partitionDataBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionDataBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadDataRate);
        EXPECT_EQ(0, collector.partitionReadMetaBlockQps);
        EXPECT_EQ(0, collector.partitionReadMetaBlockErrorQps);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadMetaRate);
    }
    { // 2. read meta from cache
        monitor::ReadMetricsCollector collector("topic", 0);
        fc.collectBlockMetrics(ERROR_NONE, "file.meta", true, block, &collector);
        EXPECT_EQ(1, collector.partitionReadBlockQps);
        EXPECT_EQ(1, collector.partitionBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadRate);
        EXPECT_EQ(0, collector.actualReadBlockCount);
        EXPECT_EQ(0, collector.partitionReadDataBlockQps);
        EXPECT_EQ(0, collector.partitionReadDataBlockErrorQps);
        EXPECT_EQ(0, collector.partitionDataBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionDataBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadDataRate);
        EXPECT_EQ(1, collector.partitionReadMetaBlockQps);
        EXPECT_EQ(0, collector.partitionReadMetaBlockErrorQps);
        EXPECT_EQ(1, collector.partitionMetaBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadMetaRate);
    }
    { // 3. read data from pangu
        monitor::ReadMetricsCollector collector("topic", 0);
        fc.collectBlockMetrics(ERROR_NONE, "file.data", false, block, &collector);
        EXPECT_EQ(1, collector.partitionReadBlockQps);
        EXPECT_EQ(0, collector.partitionBlockCacheHitTimes);
        EXPECT_EQ(1, collector.partitionBlockCacheNotHitTimes);
        EXPECT_EQ(10, collector.partitionDfsActualReadRate);
        EXPECT_EQ(1, collector.actualReadBlockCount);
        EXPECT_EQ(1, collector.partitionReadDataBlockQps);
        EXPECT_EQ(0, collector.partitionReadDataBlockErrorQps);
        EXPECT_EQ(0, collector.partitionDataBlockCacheHitTimes);
        EXPECT_EQ(1, collector.partitionDataBlockCacheNotHitTimes);
        EXPECT_EQ(10, collector.partitionDfsActualReadDataRate);
        EXPECT_EQ(0, collector.partitionReadMetaBlockQps);
        EXPECT_EQ(0, collector.partitionReadMetaBlockErrorQps);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadMetaRate);
    }
    { // 4. read meta from pangu
        monitor::ReadMetricsCollector collector("topic", 0);
        fc.collectBlockMetrics(ERROR_NONE, "file.meta", false, block, &collector);
        EXPECT_EQ(1, collector.partitionReadBlockQps);
        EXPECT_EQ(0, collector.partitionBlockCacheHitTimes);
        EXPECT_EQ(1, collector.partitionBlockCacheNotHitTimes);
        EXPECT_EQ(10, collector.partitionDfsActualReadRate);
        EXPECT_EQ(1, collector.actualReadBlockCount);
        EXPECT_EQ(0, collector.partitionReadDataBlockQps);
        EXPECT_EQ(0, collector.partitionReadDataBlockErrorQps);
        EXPECT_EQ(0, collector.partitionDataBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionDataBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadDataRate);
        EXPECT_EQ(1, collector.partitionReadMetaBlockQps);
        EXPECT_EQ(0, collector.partitionReadMetaBlockErrorQps);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheHitTimes);
        EXPECT_EQ(1, collector.partitionMetaBlockCacheNotHitTimes);
        EXPECT_EQ(10, collector.partitionDfsActualReadMetaRate);
    }
    { // 5. read data from cache fail
        monitor::ReadMetricsCollector collector("topic", 0);
        fc.collectBlockMetrics(ERROR_BROKER_BUSY, "file.data", true, block, &collector);
        EXPECT_EQ(1, collector.partitionReadBlockQps);
        EXPECT_EQ(0, collector.partitionBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadRate);
        EXPECT_EQ(0, collector.actualReadBlockCount);
        EXPECT_EQ(1, collector.partitionReadDataBlockQps);
        EXPECT_EQ(1, collector.partitionReadDataBlockErrorQps);
        EXPECT_EQ(0, collector.partitionDataBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionDataBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadDataRate);
        EXPECT_EQ(0, collector.partitionReadMetaBlockQps);
        EXPECT_EQ(0, collector.partitionReadMetaBlockErrorQps);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadMetaRate);
    }
    { // 6. read meta from pangu fail
        monitor::ReadMetricsCollector collector("topic", 0);
        fc.collectBlockMetrics(ERROR_UNKNOWN, "file.meta", false, block, &collector);
        EXPECT_EQ(1, collector.partitionReadBlockQps);
        EXPECT_EQ(0, collector.partitionBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadRate);
        EXPECT_EQ(0, collector.actualReadBlockCount);
        EXPECT_EQ(0, collector.partitionReadDataBlockQps);
        EXPECT_EQ(0, collector.partitionReadDataBlockErrorQps);
        EXPECT_EQ(0, collector.partitionDataBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionDataBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadDataRate);
        EXPECT_EQ(1, collector.partitionReadMetaBlockQps);
        EXPECT_EQ(1, collector.partitionReadMetaBlockErrorQps);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheHitTimes);
        EXPECT_EQ(0, collector.partitionMetaBlockCacheNotHitTimes);
        EXPECT_EQ(0, collector.partitionDfsActualReadMetaRate);
    }
}

} // namespace storage
} // namespace swift
