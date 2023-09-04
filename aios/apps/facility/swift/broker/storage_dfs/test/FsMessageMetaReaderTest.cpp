#include "swift/broker/storage_dfs/FsMessageMetaReader.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/FileManagerUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
namespace swift {
namespace storage {
using namespace swift::util;
using namespace swift::common;
using namespace swift::protocol;

const uint64_t INT64_MAX_VALUE = numeric_limits<uint64_t>::max();

class FsMessageMetaReaderTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}

private:
    void prepareTopicFile(const string &dfsRoot,
                          const string &topic,
                          uint32_t partCnt,
                          int64_t beginTimestamp,
                          int64_t beginMsgId,
                          uint32_t fileCount = 10)

    {
        string topicPath = dfsRoot + "/" + topic;
        fslib::ErrorCode ec = fslib::fs::FileSystem::mkDir(topicPath, true);
        ASSERT_EQ(fslib::EC_OK, ec);

        for (uint32_t i = 0; i < partCnt; ++i) {
            string partPath = topicPath + "/" + to_string(i);
            ec = fslib::fs::FileSystem::mkDir(partPath, true);
            ASSERT_EQ(fslib::EC_OK, ec);
            int64_t msgId = beginMsgId + i;
            int64_t timestamp = beginTimestamp + i;
            for (uint32_t cnt = 0; cnt < fileCount; ++cnt) {
                string prefix = partPath + "/" + FileManagerUtil::generateFilePrefix(msgId, timestamp);
                ASSERT_TRUE(prepartMeta(prefix + ".meta", timestamp, 10));
                ASSERT_TRUE(prepartData(prefix + ".data"));
                msgId += 10;
                timestamp += 10;
            }
        }
    }

    bool prepartMeta(const string &fileName, int64_t timestamp, uint32_t count) {
        string metaStr;
        for (int64_t i = 0; i < count; i++) {
            FileMessageMeta meta;
            meta.timestamp = timestamp;
            meta.endPos = i + 1;
            meta.payload = timestamp + 1;
            meta.maskPayload = timestamp + 2;
            metaStr.append((char *)(&meta), FILE_MESSAGE_META_SIZE);
            ++timestamp;
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

    bool prepartData(const string &fileName) {
        fslib::ErrorCode ec = fslib::fs::FileSystem::writeFile(fileName, "data");
        if (ec != fslib::EC_OK) {
            return false;
        }
        string data;
        fslib::fs::FileSystem::readFile(fileName, data);
        ec = fslib::fs::FileSystem::isExist(fileName);
        if (ec != fslib::EC_TRUE) {
            return false;
        }
        return true;
    }
};

TEST_F(FsMessageMetaReaderTest, testInitMetaFileLst) {
    string dfsRoot(GET_TEMPLATE_DATA_PATH());
    FsMessageMetaReader mr(dfsRoot, "topic", NULL);
    EXPECT_FALSE(mr.initMetaFileLst("", "", NULL));
    EXPECT_FALSE(mr.initMetaFileLst(dfsRoot, "", NULL));
    ASSERT_FALSE(mr.initMetaFileLst("", "topic", NULL));
    Filter filter;
    filter.set_from(100);
    filter.set_to(50);
    ASSERT_FALSE(mr.initMetaFileLst(dfsRoot, "topic", &filter));
    ASSERT_FALSE(mr.initMetaFileLst(dfsRoot, "topic", NULL));

    prepareTopicFile(dfsRoot, "topic", 4, 100, 10);
    EXPECT_TRUE(mr.initMetaFileLst(dfsRoot, "topic", NULL));
    EXPECT_EQ(4, mr._metaFileMap.size());
    for (uint32_t part = 0; part < 4; ++part) {
        cout << "part " << part << ":" << endl;
        const vector<MetaFileInfo> &info = mr._metaFileMap[part];
        for (size_t cnt = 0; cnt < info.size(); ++cnt) {
            EXPECT_EQ(100 + part + 10 * cnt, info[cnt].timestamp);
            EXPECT_EQ(10 + part + 10 * cnt, info[cnt].beginMsgId);
            EXPECT_EQ(info[cnt].beginMsgId + 10, info[cnt].endMsgId);
            cout << info[cnt].name << " " << info[cnt].timestamp << " " << info[cnt].beginMsgId << " "
                 << info[cnt].endMsgId << endl;
        }
        cout << endl;
    }
}

TEST_F(FsMessageMetaReaderTest, testFindMetaFileIndex) {
    FsMessageMetaReader mr("dfs://aa/bb", "topic", NULL);
    vector<MetaFileInfo> mfi;
    size_t beginPos = 100, endPos = 100;
    EXPECT_FALSE(mr.findMetaFileIndex(mfi, 0, 0, beginPos, endPos));

    for (int i = 0; i < 10; ++i) {
        MetaFileInfo info(to_string(i) + ".meta", i * 100, 0, 1);
        mfi.push_back(info);
    }
    EXPECT_TRUE(mr.findMetaFileIndex(mfi, 0, INT64_MAX_VALUE, beginPos, endPos));
    EXPECT_EQ(0, beginPos);
    EXPECT_EQ(10, endPos);

    EXPECT_TRUE(mr.findMetaFileIndex(mfi, 100, 500, beginPos, endPos));
    EXPECT_EQ(1, beginPos);
    EXPECT_EQ(6, endPos);

    EXPECT_TRUE(mr.findMetaFileIndex(mfi, 1, 501, beginPos, endPos));
    EXPECT_EQ(0, beginPos);
    EXPECT_EQ(6, endPos);

    EXPECT_TRUE(mr.findMetaFileIndex(mfi, 1, 499, beginPos, endPos));
    EXPECT_EQ(0, beginPos);
    EXPECT_EQ(5, endPos);

    EXPECT_TRUE(mr.findMetaFileIndex(mfi, 55, 55, beginPos, endPos));
    EXPECT_EQ(0, beginPos);
    EXPECT_EQ(1, endPos);

    EXPECT_TRUE(mr.findMetaFileIndex(mfi, 55, 1000, beginPos, endPos));
    EXPECT_EQ(0, beginPos);
    EXPECT_EQ(10, endPos);
}

void expectMsgMeta(
    const MessageMeta &meta, int64_t msgId, int64_t timestamp, uint16_t payload, uint8_t mask, bool isMerged = false) {
    EXPECT_EQ(msgId, meta.msgId);
    EXPECT_EQ(timestamp, meta.timestamp);
    EXPECT_EQ(payload, meta.payload);
    EXPECT_EQ(mask, meta.mask);
    EXPECT_EQ(isMerged, meta.isMerged);
}

TEST_F(FsMessageMetaReaderTest, testReadMeta) {
    string dfsRoot(GET_TEMPLATE_DATA_PATH());
    prepareTopicFile(dfsRoot, "topic", 4, 100, 10);
    { // read all
        FsMessageMetaReader mr(dfsRoot, "topic", NULL);
        ASSERT_EQ(4, mr._metaFileMap.size());
        vector<MessageMeta> outMetaVec;
        ASSERT_TRUE(mr.readMeta(outMetaVec));
        ASSERT_EQ(400, outMetaVec.size());
        int64_t expectIdStart = 10, expectTsStart = 100;
        uint32_t repeatTimes = 1, inital = 1;
        for (size_t index = 0; index < outMetaVec.size() - 3; ++index) {
            const MessageMeta &meta = outMetaVec[index];
            if (0 == repeatTimes) {
                ++expectIdStart;
                ++expectTsStart;
                repeatTimes = ++inital;
                repeatTimes = repeatTimes < 4 ? repeatTimes : 4;
            }
            expectMsgMeta(meta, expectIdStart, expectTsStart, expectTsStart + 1, expectTsStart + 2, false);
            --repeatTimes;
        }
        // expect last 3
        expectMsgMeta(outMetaVec[397], 111, 201, 202, 203);
        expectMsgMeta(outMetaVec[398], 111, 201, 202, 203);
        expectMsgMeta(outMetaVec[399], 112, 202, 203, 204);
    }
    { // read filter
        Filter filter;
        filter.set_from(100);
        filter.set_to(200);
        filter.set_uint8filtermask(2);
        filter.set_uint8maskresult(2);
        FsMessageMetaReader mr(dfsRoot, "topic", &filter);
        ASSERT_EQ(1, mr._metaFileMap.size());
        vector<MessageMeta> outMetaVec;
        ASSERT_TRUE(mr.readMeta(outMetaVec));
        ASSERT_EQ(50, outMetaVec.size());
        for (const auto &meta : outMetaVec) {
            EXPECT_TRUE(meta.payload >= 100 && meta.payload <= 200);
            EXPECT_TRUE((meta.mask & 2) == 2);
        }

        // add time range
        ASSERT_TRUE(mr.readMeta(outMetaVec, 110, 180));
        ASSERT_EQ(35, outMetaVec.size());
        for (const auto &meta : outMetaVec) {
            EXPECT_TRUE(meta.timestamp >= 110 && meta.timestamp <= 180);
            EXPECT_TRUE(meta.payload >= 100 && meta.payload <= 200);
            EXPECT_TRUE((meta.mask & 2) == 2);
        }

        // empty
        ASSERT_TRUE(mr.readMeta(outMetaVec, 11, 99));
        ASSERT_EQ(0, outMetaVec.size());
    }
}

} // namespace storage
} // namespace swift
