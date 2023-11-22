#include "indexlib/index/kkv/test/value_inline_kkv_data_dumper_unittest.h"

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "indexlib/common/chunk/chunk_strategy.h"
#include "indexlib/config/test/region_schema_maker.h"
#include "indexlib/index/kkv/normal_kkv_data_dumper.h"
#include "indexlib/util/SimplePool.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::test;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(kkv, ValueInlineKKVDataDumperTest);

namespace {

class MockChunkStrategy : public ChunkStrategy
{
public:
    MOCK_METHOD(bool, NeedFlush, (const ChunkWriterPtr& writer), (const, override));
};

} // namespace

ValueInlineKKVDataDumperTest::ValueInlineKKVDataDumperTest() {}

ValueInlineKKVDataDumperTest::~ValueInlineKKVDataDumperTest() {}

void ValueInlineKKVDataDumperTest::CaseSetUp() {}

void ValueInlineKKVDataDumperTest::CaseTearDown() {}

void ValueInlineKKVDataDumperTest::TestSimpleProcess() { InnerTestSimpleProcess("0:3", "", "0,0,0"); }

void ValueInlineKKVDataDumperTest::InnerTestSimpleProcess(const string& skeyNodeInfoStr,
                                                          const string& resultStrBeforeClose,
                                                          const string& resultStrAfterClose)
{
    tearDown();
    setUp();
    string field = "pkey:uint64;skey:uint32;value:uint32;";
    string field1 = "pkey:uint64;skey:string;value:string;";
    string field2 = "pkey:uint64;skey:uint64;value:uint32;";
    RegionSchemaMaker maker(field, "kkv");
    maker.AddRegion("region1", "pkey", "skey", "value", field1);
    maker.AddRegion("region2", "pkey", "skey", "value", field2);
    auto schema = maker.GetSchema();
    auto regionId = schema->GetRegionId("region2");
    auto kkvConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema(regionId)->GetPrimaryKeyIndexConfig());
    kkvConfig->GetIndexPreference().TEST_SetValueInline(true);

    auto directory = GET_SEGMENT_DIRECTORY();
    SimplePool simplePool;

    ValueInlineKKVDataDumper<uint64_t> dumper(schema, kkvConfig, true, directory, &simplePool);
    // NormalKKVDataDumper<uint64_t> dumper(schema, kkvConfig, true, directory, &simplePool);

    // set mock chunk strategy
    MockChunkStrategy* mockChunkStrategy(new MockChunkStrategy);
    ChunkStrategyPtr chunkStrategy(mockChunkStrategy);
    dumper.mChunkStrategy = chunkStrategy;
    // dumper.mSKeyDumper->mChunkStrategy = chunkStrategy;
    EXPECT_CALL(*mockChunkStrategy, NeedFlush(_)).WillRepeatedly(Return(false));

    auto pKeyIter = dumper.GetPKeyOffsetIterator();

    vector<string> chunkStrs = StringUtil::split(skeyNodeInfoStr, "#");
    for (size_t i = 0; i < chunkStrs.size(); ++i) {
        const auto& chunkStr = chunkStrs[i];
        vector<vector<uint64_t>> inChunkInfos;
        StringUtil::fromString(chunkStr, inChunkInfos, ":", ";");
        for (size_t j = 0; j < inChunkInfos.size(); ++j) {
            const auto& inChunkInfo = inChunkInfos[j];
            uint64_t pkey = inChunkInfo[0];
            uint64_t count = inChunkInfo[1];
            assert(count > 0);
            for (size_t idx = 0; idx < count; ++idx) {
                bool lastSKey = (idx == count - 1);
                // if (i != chunkStrs.size() - 1 && j == inChunkInfos.size() - 1 && lastSKey)
                // {
                //     // last key last skey will flush
                //     EXPECT_CALL(*mockChunkStrategy, NeedFlush(_))
                //         .WillOnce(Return(true));
                // }
                // else
                // {
                //     EXPECT_CALL(*mockChunkStrategy, NeedFlush(_))
                //         .WillOnce(Return(false));
                // }
                string valueStr = std::to_string(idx);
                dumper.Collect(pkey, idx, 1, UNINITIALIZED_EXPIRE_TIME, false, false, lastSKey, StringView(valueStr),
                               regionId);
            }
        }
    }
    CheckIter(pKeyIter, resultStrBeforeClose, regionId);
    dumper.Close();
    CheckIter(pKeyIter, resultStrAfterClose, regionId);
}

void ValueInlineKKVDataDumperTest::CheckIter(const DumpablePKeyOffsetIteratorPtr& pkeyIter, const string& resultStr,
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
