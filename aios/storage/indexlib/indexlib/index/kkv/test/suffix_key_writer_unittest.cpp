#include "indexlib/index/kkv/test/suffix_key_writer_unittest.h"

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/common/chunk/chunk_strategy.h"
#include "indexlib/util/SimplePool.h"

using namespace autil;
using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SuffixKeyWriterTest);

SuffixKeyWriterTest::SuffixKeyWriterTest() {}

SuffixKeyWriterTest::~SuffixKeyWriterTest() {}

void SuffixKeyWriterTest::CaseSetUp() {}

void SuffixKeyWriterTest::CaseTearDown() {}

void SuffixKeyWriterTest::TestLinkSkeyNode()
{
    InnerTestLinkSkeyNode(200, 9);
    InnerTestLinkSkeyNode(400, 25);
    InnerTestLinkSkeyNode(1000, 100);

    // no skip list
    InnerTestLinkSkeyNode(4000, 10000);
}

void SuffixKeyWriterTest::MakeData(SKeyNodeVector& skeyNodes, uint32_t count)
{
    uint32_t cursor = 0;
    for (size_t i = 0; i < count; i++) {
        skeyNodes.push_back(SKeyWriter::SKeyNode(i, cursor, cursor, UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET));
        ++cursor;

        if (i % 3 == 0) {
            skeyNodes.push_back(
                SKeyWriter::SKeyNode(i, cursor, cursor, UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET));
            ++cursor;
        }
    }
    random_shuffle(skeyNodes.begin(), skeyNodes.end());
}

void SuffixKeyWriterTest::MakeResult(const SKeyNodeVector& skeyNodes, SKeyNodeVector& result)
{
    bool hasPkeyDeleted = false;
    SKeyWriter::SKeyNode delPkeyNode(0, 0, 0, UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET);

    map<uint32_t, SKeyWriter::SKeyNode> sKeyMap;
    for (size_t i = 0; i < skeyNodes.size(); i++) {
        if (skeyNodes[i].offset == SKEY_ALL_DELETED_OFFSET) {
            hasPkeyDeleted = true;
            delPkeyNode = skeyNodes[i];
            sKeyMap.clear();
            continue;
        }

        auto iter = sKeyMap.find(skeyNodes[i].skey);
        if (iter == sKeyMap.end()) {
            sKeyMap.insert(make_pair(skeyNodes[i].skey, skeyNodes[i]));
        } else {
            iter->second = skeyNodes[i];
        }
    }

    if (hasPkeyDeleted) {
        result.push_back(delPkeyNode);
    }
    for (auto it = sKeyMap.begin(); it != sKeyMap.end(); it++) {
        result.push_back(it->second);
    }
}

void SuffixKeyWriterTest::CheckResult(const SKeyWriter& skeyWriter, const SKeyListInfo& listInfo,
                                      const SKeyNodeVector& result)
{
    SKeyNodeVector skeyNodes;
    uint32_t cursor = listInfo.skeyHeader;
    while (cursor != INVALID_SKEY_OFFSET) {
        const SKeyWriter::SKeyNode& node = skeyWriter.GetSKeyNode(cursor);
        skeyNodes.push_back(node);
        cursor = node.next;
    }

    // check skey list
    ASSERT_EQ(skeyNodes.size(), result.size());
    for (size_t i = 0; i < skeyNodes.size(); i++) {
        ASSERT_EQ(result[i].skey, skeyNodes[i].skey);
        ASSERT_EQ(result[i].timestamp, skeyNodes[i].timestamp);
        ASSERT_EQ(result[i].offset, skeyNodes[i].offset);
    }

    // check skip list
    uint32_t listNodeCount = 0;
    uint32_t maxListCount = 0;
    size_t count = 0;
    uint32_t listCursor = listInfo.blockHeader;
    uint32_t preSkey = 0;
    while (listCursor != INVALID_SKEY_OFFSET) {
        const SKeyWriter::ListNode& listNode = skeyWriter.mListNodes[listCursor];
        ASSERT_EQ(listNode.skey, skeyWriter.GetSKeyNode(listNode.skeyOffset).skey);
        if (count != 0) {
            ASSERT_TRUE(listNode.skey > preSkey);
        }
        preSkey = listNode.skey;
        count += listNode.count;
        listCursor = listNode.next;
        ++listNodeCount;
        maxListCount = max(listNode.count, maxListCount);
    }

    cout << "####################" << endl;
    cout << "list node count:" << listNodeCount << endl;
    cout << "max list count:" << maxListCount << endl;
    if (count > 0) {
        ASSERT_EQ(count, skeyNodes.size());
    }

    int64_t begin = TimeUtility::currentTime();
    // check seek target skey
    cursor = listInfo.skeyHeader;
    listCursor = listInfo.blockHeader;
    for (size_t i = 0; i < skeyNodes.size(); i++) {
        if (skeyNodes[i].offset == SKEY_ALL_DELETED_OFFSET) {
            continue;
        }

        SKeyWriter::SKeyNode seekNode(0, 0, 0, UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET);
        ASSERT_TRUE(skeyWriter.SeekTargetSKey(skeyNodes[i].skey, seekNode, listCursor, cursor));
        ASSERT_EQ(seekNode.skey, skeyNodes[i].skey);
        ASSERT_EQ(seekNode.timestamp, skeyNodes[i].timestamp);
        ASSERT_EQ(seekNode.offset, skeyNodes[i].offset);
    }
    int64_t end = TimeUtility::currentTime();
    cout << "seek time :" << end - begin << endl;
}

void SuffixKeyWriterTest::InnerTestLinkSkeyNode(uint32_t count, uint32_t threshold)
{
    int64_t reserveSize = count * sizeof(SKeyWriter::SKeyNode) * 10;
    SKeyWriter skeyWriter;
    skeyWriter.Init(reserveSize, threshold);

    SKeyNodeVector skeyNodes;
    MakeData(skeyNodes, count);

    // create skey list without delete pkey
    SKeyListInfo listInfo;
    listInfo.skeyHeader = skeyWriter.Append(skeyNodes[0]);
    listInfo.count = 1;
    listInfo.blockHeader = INVALID_SKEY_OFFSET;
    for (size_t i = 1; i < skeyNodes.size(); i++) {
        uint32_t skeyOffset = skeyWriter.Append(skeyNodes[i]);
        skeyWriter.LinkSkeyNode(listInfo, skeyOffset);
    }
    SKeyNodeVector results;
    MakeResult(skeyNodes, results);
    CheckResult(skeyWriter, listInfo, results);

    // delete pkey
    SKeyWriter::SKeyNode delPkeyNode(0, SKEY_ALL_DELETED_OFFSET, 0, UNINITIALIZED_EXPIRE_TIME, INVALID_SKEY_OFFSET);
    uint32_t skeyOffset = skeyWriter.Append(delPkeyNode);
    skeyWriter.LinkSkeyNode(listInfo, skeyOffset);

    ASSERT_EQ(skeyOffset, listInfo.skeyHeader);
    ASSERT_EQ(INVALID_SKEY_OFFSET, listInfo.blockHeader);
    ASSERT_EQ((uint32_t)1, listInfo.count);

    int64_t begin = TimeUtility::currentTime();
    // append list after delete skey
    for (size_t i = 0; i < skeyNodes.size(); i++) {
        uint32_t skeyOffset = skeyWriter.Append(skeyNodes[i]);
        skeyWriter.LinkSkeyNode(listInfo, skeyOffset);
    }
    int64_t end = TimeUtility::currentTime();
    cout << "----------------------" << endl;
    cout << "uniq skey :" << results.size() << endl;
    cout << "Append qps:" << skeyNodes.size() / ((double)(end - begin) / 1000000) << endl;

    SKeyNodeVector newResult;
    newResult.push_back(delPkeyNode);
    newResult.insert(newResult.end(), results.begin(), results.end());
    CheckResult(skeyWriter, listInfo, newResult);
}
}} // namespace indexlib::index
