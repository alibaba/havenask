#include "indexlib/index/kkv/test/suffix_key_dumper_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/common/chunk/chunk_strategy.h"
#include "indexlib/index_define.h"
#include "indexlib/util/SimplePool.h"

using namespace autil;
using namespace std;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SuffixKeyDumperTest);

namespace {
class MockDumpableSKeyNodeIterator : public DumpableSKeyNodeIterator
{
public:
    MOCK_METHOD(bool, IsValid, (), (const, override));
    MOCK_METHOD(void, MoveToNext, (), (override));
    MOCK_METHOD(uint64_t, Get, (StringView & value), (const, override));
    MOCK_METHOD(regionid_t, GetRegionId, (), (const, override));
};

class MockChunkStrategy : public ChunkStrategy
{
public:
    MOCK_METHOD(bool, NeedFlush, (const ChunkWriterPtr& writer), (const, override));
};
}; // namespace

SuffixKeyDumperTest::SuffixKeyDumperTest() {}

SuffixKeyDumperTest::~SuffixKeyDumperTest() {}

void SuffixKeyDumperTest::CaseSetUp() {}

void SuffixKeyDumperTest::CaseTearDown() {}

void SuffixKeyDumperTest::TestSimpleProcess()
{
    InnerTestSimpleProcess("0:3", "", "0,0,0", 34);
    InnerTestSimpleProcess("0:3;1:10#1:5;2:10", "0,0,0;1,0,30", "1,134,0;2,134,50", 288);
}

void SuffixKeyDumperTest::InnerTestSimpleProcess(const string& skeyNodeInfoStr, const string& resultStrBeforeClose,
                                                 const string& resultStrAfterClose, size_t dataLen)
{
    tearDown();
    setUp();

    static const string SKEY_NODE_VALUE_STR = string("0123456789", 10);
    static const StringView RET_VALUE = StringView(SKEY_NODE_VALUE_STR);
    static const regionid_t RET_REGIONID = 7;

    MockDumpableSKeyNodeIterator* mockIter = new MockDumpableSKeyNodeIterator();
    DumpableSKeyNodeIteratorPtr iter(mockIter);

    SimplePool simplePool;
    SuffixKeyDumper dumper(&simplePool);
    dumper.Init(KKVIndexPreference(), iter, GET_SEGMENT_DIRECTORY());
    DumpablePKeyOffsetIteratorPtr pkeyIter = dumper.CreatePkeyOffsetIterator();

    // set mock chunk strategy
    MockChunkStrategy* mockChunkStrategy(new MockChunkStrategy);
    ChunkStrategyPtr chunkStrategy(mockChunkStrategy);
    dumper.mChunkStrategy = chunkStrategy;

    vector<string> chunkStrs = StringUtil::split(skeyNodeInfoStr, "#");
    for (size_t i = 0; i < chunkStrs.size(); i++) {
        vector<vector<uint64_t>> inChunkInfos;
        StringUtil::fromString(chunkStrs[i], inChunkInfos, ":", ";");

        // process each skey
        for (size_t j = 0; j < inChunkInfos.size(); j++) {
            uint64_t pkey = inChunkInfos[j][0];
            uint32_t count = inChunkInfos[j][1];

            assert(count > 0);
            auto& obj = EXPECT_CALL(*mockIter, IsValid()).WillOnce(Return(true));
            for (uint32_t idx = 0; idx < count - 1; idx++) {
                obj.WillOnce(Return(true));
            }
            obj.WillOnce(Return(false));

            EXPECT_CALL(*mockIter, Get(_))
                .Times(count)
                .WillRepeatedly(DoAll(SetArgReferee<0>(RET_VALUE), Return(pkey)));
            EXPECT_CALL(*mockIter, GetRegionId()).WillOnce(Return(RET_REGIONID));
            EXPECT_CALL(*mockIter, MoveToNext()).Times(count);
            if (i != chunkStrs.size() - 1 && j == inChunkInfos.size() - 1) {
                // last key last skey will need flush
                if (count == 1) {
                    EXPECT_CALL(*mockChunkStrategy, NeedFlush(_)).WillOnce(Return(true));
                } else {
                    auto& obj = EXPECT_CALL(*mockChunkStrategy, NeedFlush(_)).WillOnce(Return(false));
                    bool needFlush = false;
                    for (size_t idx = 0; idx < count - 1; idx++) {
                        if (idx == count - 2) // last
                        {
                            needFlush = true;
                        }
                        obj.WillOnce(Return(needFlush));
                    }
                }
            } else {
                EXPECT_CALL(*mockChunkStrategy, NeedFlush(_)).Times(count).WillRepeatedly(Return(false));
            }
            dumper.Flush();
        }
    }

    CheckIter(pkeyIter, resultStrBeforeClose, RET_REGIONID);
    EXPECT_CALL(*mockIter, IsValid()).WillRepeatedly(Return(false));
    dumper.Close();
    CheckIter(pkeyIter, resultStrAfterClose, RET_REGIONID);

    FileReaderPtr fileReader = GET_SEGMENT_DIRECTORY()->CreateFileReader(SUFFIX_KEY_FILE_NAME, FSOT_MEM);
    ASSERT_EQ(dataLen, fileReader->GetLength());
}

void SuffixKeyDumperTest::CheckIter(const DumpablePKeyOffsetIteratorPtr& pkeyIter, const string& resultStr,
                                    regionid_t regionId)
{
    vector<vector<uint64_t>> pkeyIterInfos;
    StringUtil::fromString(resultStr, pkeyIterInfos, ",", ";");
    for (size_t i = 0; i < pkeyIterInfos.size(); i++) {
        assert(pkeyIterInfos[i].size() == 3);
        ASSERT_TRUE(pkeyIter->IsValid());
        OnDiskPKeyOffset offset;
        uint64_t pkey = pkeyIter->Get(offset);
        ASSERT_EQ(pkeyIterInfos[i][0], pkey);
        ASSERT_EQ(pkeyIterInfos[i][1], offset.chunkOffset);
        ASSERT_EQ(pkeyIterInfos[i][2], offset.inChunkOffset);
        ASSERT_EQ(regionId, offset.regionId);
        pkeyIter->MoveToNext();
    }
    ASSERT_FALSE(pkeyIter->IsValid());
}
}} // namespace indexlib::index
