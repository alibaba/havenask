#include "swift/util/ReaderRec.h"

#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <utility>

#include "autil/TimeUtility.h"
#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace util {

class ReaderDesTest : public TESTBASE {};
class ReaderInfoTest : public TESTBASE {};

TEST_F(ReaderDesTest, testReaderDes) {
    { // default value
        ReaderDes reader;
        EXPECT_EQ(0, reader.ip);
        EXPECT_EQ(0, reader.port);
        EXPECT_EQ(0, reader.from);
        EXPECT_EQ(0, reader.to);
        EXPECT_EQ(0, reader.padding);
        EXPECT_EQ("0.0.0.0:0, from:0, to:0", reader.toString());
    }
    { // normal
        ReaderDes reader;
        reader.setIpPort("111.15.87.152:51807");
        reader.from = 10;
        reader.to = 20;
        EXPECT_EQ(1863276440, reader.ip);
        EXPECT_EQ(51807, reader.port);
        EXPECT_EQ("111.15.87.152:51807, from:10, to:20", reader.toString());
    }
    { // address format error
        ReaderDes reader;
        reader.setIpPort("15.87.152:51807");
        reader.from = 10;
        reader.to = 20;
        EXPECT_EQ(0, reader.ip);
        EXPECT_EQ(0, reader.port);
        EXPECT_EQ("0.0.0.0:0, from:10, to:20", reader.toString());
    }
}

TEST_F(ReaderInfoTest, testReaderInfo) {
    { // default value
        ReadFileInfo info;
        EXPECT_EQ(-1, info.blockId);
        EXPECT_TRUE(info.fileName.empty());
        EXPECT_FALSE(info._isReadFile);
        EXPECT_FALSE(info._lastIsReadFile);
        EXPECT_EQ(0, info._latedData.size());
    }
    { // default value
        ReaderInfo info;
        EXPECT_EQ(0, info.nextTimestamp);
        EXPECT_EQ(0, info.nextMsgId);
        EXPECT_EQ(0, info.requestTime);
        EXPECT_TRUE(info.metaInfo != nullptr);
        EXPECT_TRUE(info.dataInfo != nullptr);
        EXPECT_FALSE(info.getReadFile());
        EXPECT_EQ("requestTime:0,readFile:0,nextTimestamp:0,nextMsgId:0,metaFile:,"
                  "metaBlock:-1,metaRate:0,dataFile:,dataBlock:-1,dataRate:0",
                  info.toString());
    }
    {
        ReaderInfo info(11);
        EXPECT_EQ(0, info.nextTimestamp);
        EXPECT_EQ(0, info.nextMsgId);
        EXPECT_EQ(11, info.requestTime);
        EXPECT_EQ(-1, info.metaInfo->blockId);
        EXPECT_EQ(-1, info.dataInfo->blockId);
        EXPECT_FALSE(info.getReadFile());
        EXPECT_EQ("requestTime:11,readFile:0,nextTimestamp:0,nextMsgId:0,metaFile:,"
                  "metaBlock:-1,metaRate:0,dataFile:,dataBlock:-1,dataRate:0",
                  info.toString());
    }
    {
        ReaderInfo info(11);
        info.nextTimestamp = 10;
        info.nextMsgId = 20;
        info.requestTime = 30;
        info.metaInfo->blockId = 40;
        info.dataInfo->blockId = 50;
        info.metaInfo->fileName = "/topic1/111.meta";
        info.dataInfo->fileName = "/topic1/111.data";
        info.setReadFile();
        EXPECT_EQ("requestTime:30,readFile:1,nextTimestamp:10,nextMsgId:20,"
                  "metaFile:/topic1/111.meta,metaBlock:40,metaRate:0,"
                  "dataFile:/topic1/111.data,dataBlock:50,dataRate:0",
                  info.toString());
    }
}

TEST_F(ReaderInfoTest, testCalcRate) {
    ReadFileInfo info;
    EXPECT_EQ(0, info.getReadRate());

    info.updateRate(10);
    EXPECT_EQ(10, info.getReadRate());
    int64_t curTime = autil::TimeUtility::currentTime() + 1;
    info.clearHistoryData(curTime);

    for (int count = 0; count < 10; ++count) {
        usleep(100000);
        info.updateRate(count * 10);
    }
    EXPECT_EQ(450, info.getReadRate());

    usleep(500000);
    EXPECT_EQ(350, info.getReadRate());

    usleep(500000);
    EXPECT_EQ(7, info.getReadRate());
}

TEST_F(ReaderInfoTest, testGetSlowestMemReaderInfo) {
    int64_t MAX_VALUE = numeric_limits<int64_t>::max();
    {
        ReaderInfoMap rimap;
        int64_t timestamp, msgId;
        getSlowestMemReaderInfo(&rimap, timestamp, msgId);
        EXPECT_EQ(MAX_VALUE, timestamp);
        EXPECT_EQ(MAX_VALUE, msgId);
    }
    {
        // one reader
        ReaderInfoMap rimap;
        ReaderDes r1("10.1.1.1:332", 100, 200);
        ReaderInfoPtr i1(new ReaderInfo(10, 20, 153111, 30, 40, "aa.meta", "aa.data", true));
        rimap[r1] = i1;
        int64_t timestamp, msgId;
        getSlowestMemReaderInfo(&rimap, timestamp, msgId);
        EXPECT_EQ(MAX_VALUE, timestamp);
        EXPECT_EQ(MAX_VALUE, msgId);

        i1->setReadFile(false);
        getSlowestMemReaderInfo(&rimap, timestamp, msgId);
        EXPECT_EQ(10, timestamp);
        EXPECT_EQ(20, msgId);

        // many readers
        ReaderDes r2("10.1.1.1:333", 100, 200);
        ReaderDes r3("10.1.1.1:332", 101, 200);
        ReaderDes r4("10.1.1.1:332", 100, 201);
        ReaderInfoPtr i2(new ReaderInfo(8, 20, 153222, 30, 40, "aa.meta", "aa.data", false));
        ReaderInfoPtr i3(new ReaderInfo(7, 16, 153333, 30, 40, "aa.meta", "aa.data", true));
        ReaderInfoPtr i4(new ReaderInfo(9, 18, 153444, 30, 40, "aa.meta", "aa.data", false));
        rimap[r2] = i2;
        rimap[r3] = i3;
        rimap[r4] = i4;
        getSlowestMemReaderInfo(&rimap, timestamp, msgId);
        EXPECT_EQ(8, timestamp);
        EXPECT_EQ(18, msgId);

        i3->setReadFile(false);
        for (auto iter = rimap.begin(); iter != rimap.end(); ++iter) {
            cout << iter->second->toString() << endl;
        }
        getSlowestMemReaderInfo(&rimap, timestamp, msgId);
        EXPECT_EQ(7, timestamp);
        EXPECT_EQ(16, msgId);
    }
}

TEST_F(ReaderInfoTest, testGetSlowestFileReaderInfo) {
    int64_t MAX_VALUE = numeric_limits<int64_t>::max();
    const string MAX_STRING_NAME(3, char(255));
    {
        ReaderInfoMap rimap;
        string metaFileName, dataFileName;
        int64_t metaBlockId, dataBlockId;
        getSlowestFileReaderInfo(&rimap, metaFileName, metaBlockId, dataFileName, dataBlockId);
        EXPECT_EQ(MAX_STRING_NAME, metaFileName);
        EXPECT_EQ(MAX_STRING_NAME, dataFileName);
        EXPECT_EQ(MAX_VALUE, metaBlockId);
        EXPECT_EQ(MAX_VALUE, dataBlockId);
    }
    {
        // one reader
        ReaderInfoMap rimap;
        ReaderDes r1("10.1.1.1:332", 100, 200);
        ReaderInfoPtr i1(new ReaderInfo(10,
                                        20,
                                        153111,
                                        30,
                                        40,
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022888.meta",
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022888.data",
                                        false));
        rimap[r1] = i1;
        string metaFileName, dataFileName;
        int64_t metaBlockId, dataBlockId;
        getSlowestFileReaderInfo(&rimap, metaFileName, metaBlockId, dataFileName, dataBlockId);
        EXPECT_EQ(MAX_STRING_NAME, metaFileName);
        EXPECT_EQ(MAX_STRING_NAME, dataFileName);
        EXPECT_EQ(MAX_VALUE, metaBlockId);
        EXPECT_EQ(MAX_VALUE, dataBlockId);

        i1->setReadFile();
        getSlowestFileReaderInfo(&rimap, metaFileName, metaBlockId, dataFileName, dataBlockId);
        EXPECT_EQ("2020-03-11-16-12-53.022346.0000000000000000.1583914373022888.meta", metaFileName);
        EXPECT_EQ("2020-03-11-16-12-53.022346.0000000000000000.1583914373022888.data", dataFileName);
        EXPECT_EQ(30, metaBlockId);
        EXPECT_EQ(40, dataBlockId);

        // many readers
        ReaderDes r2("10.1.1.1:333", 100, 200);
        ReaderDes r3("10.1.1.1:332", 101, 200);
        ReaderDes r4("10.1.1.1:332", 100, 201);
        ReaderInfoPtr i2(new ReaderInfo(8,
                                        20,
                                        153222,
                                        22,
                                        39,
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.meta",
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.data",
                                        false));
        ReaderInfoPtr i3(new ReaderInfo(7,
                                        16,
                                        153333,
                                        23,
                                        38,
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.meta",
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.data",
                                        true));
        ReaderInfoPtr i4(new ReaderInfo(9,
                                        18,
                                        153444,
                                        24,
                                        34,
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022888.meta",
                                        "2020-03-11-16-12-53.022346.0000000000000000.1583914373022888.data",
                                        true));
        rimap[r2] = i2;
        rimap[r3] = i3;
        rimap[r4] = i4;
        getSlowestFileReaderInfo(&rimap, metaFileName, metaBlockId, dataFileName, dataBlockId);
        EXPECT_EQ("2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.meta", metaFileName);
        EXPECT_EQ("2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.data", dataFileName);
        EXPECT_EQ(23, metaBlockId);
        EXPECT_EQ(38, dataBlockId);

        i2->setReadFile();
        getSlowestFileReaderInfo(&rimap, metaFileName, metaBlockId, dataFileName, dataBlockId);
        EXPECT_EQ("2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.meta", metaFileName);
        EXPECT_EQ("2020-03-11-16-12-53.022346.0000000000000000.1583914373022222.data", dataFileName);
        EXPECT_EQ(22, metaBlockId);
        EXPECT_EQ(38, dataBlockId);
    }
}

} // namespace util
} // namespace swift
