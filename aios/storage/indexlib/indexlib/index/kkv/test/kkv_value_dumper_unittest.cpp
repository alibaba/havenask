#include "indexlib/index/kkv/test/kkv_value_dumper_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/index/kkv/on_disk_skey_node.h"
#include "indexlib/index/kkv/test/kkv_value_dumper_unittest.h"
#include "indexlib/util/SimplePool.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVValueDumperTest);

class MockChunkStrategy : public common::ChunkStrategy
{
public:
    MOCK_METHOD(bool, NeedFlush, (const common::ChunkWriterPtr& writer), (const, override));
};

KKVValueDumperTest::KKVValueDumperTest() {}

KKVValueDumperTest::~KKVValueDumperTest() {}

void KKVValueDumperTest::CaseSetUp() {}

void KKVValueDumperTest::CaseTearDown() {}

void KKVValueDumperTest::TestSimpleProcess()
{
    SimplePool simplePool;
    KKVValueDumper<uint64_t, 4> dumper(&simplePool, true);

    KKVIndexConfigPtr kkvIndexConfig(new KKVIndexConfig("", it_kkv));
    ValueConfigPtr valueConfig(new ValueConfig());
    kkvIndexConfig->SetValueConfig(valueConfig);
    dumper.Init(kkvIndexConfig, GET_SEGMENT_DIRECTORY());
    DumpableSKeyNodeIteratorPtr iter = dumper.CreateSkeyNodeIterator();

    // set mock chunk strategy
    MockChunkStrategy* mockChunkStrategy(new MockChunkStrategy);
    ChunkStrategyPtr chunkStrategy(mockChunkStrategy);
    dumper.mChunkStrategy = chunkStrategy;

    regionid_t regionId = 3;

    AddItem(dumper, mockChunkStrategy, 1, 0, 0, IT_DEL_PKEY, "", false, false, regionId);
    // NODE_DELETE_PKEY_OFFSET = 34359738365
    CheckIterator(iter, "1,0,0,false,34359738365,0,3");

    regionId++;
    AddItem(dumper, mockChunkStrategy, 1, 11, 1, IT_ADD, "abc", true, false, regionId);
    CheckIterator(iter, "");

    regionId++;
    AddItem(dumper, mockChunkStrategy, 2, 21, 2, IT_DEL_SKEY, "", true, false, regionId);
    CheckIterator(iter, "");

    regionId++;
    AddItem(dumper, mockChunkStrategy, 3, 31, 3, IT_ADD, "efg", false, true, regionId);

    // NODE_DELETE_SKEY_OFFSET = 34359738366
    CheckIterator(iter, "1,11,1,true,0,0,4;"
                        "2,21,2,true,34359738366,0,5;"
                        "3,31,3,false,0,4,6");

    regionId++;
    AddItem(dumper, mockChunkStrategy, 3, 32, 4, IT_ADD, "higk", true, false, regionId);
    CheckIterator(iter, "");

    dumper.Close();
    CheckIterator(iter, "3,32,4,true,2,0,7");

    CheckData("8:abc,efg;5:higk");
}

void KKVValueDumperTest::AddItem(KKVValueDumper<uint64_t, 4>& dumper, MockChunkStrategy* mockStrategy, uint64_t pkey,
                                 uint64_t skey, uint32_t ts, ItemType itemType, const std::string& value,
                                 bool isLastNode, bool needFlush, regionid_t regionId)
{
    EXPECT_CALL(*mockStrategy, NeedFlush(_)).WillOnce(Return(needFlush));

    switch (itemType) {
    case IT_DEL_PKEY:
        dumper.Insert(pkey, skey, ts, UNINITIALIZED_EXPIRE_TIME, true, false, isLastNode,
                      autil::StringView::empty_instance(), regionId, -1);
        break;
    case IT_DEL_SKEY:
        dumper.Insert(pkey, skey, ts, UNINITIALIZED_EXPIRE_TIME, false, true, isLastNode,
                      autil::StringView::empty_instance(), regionId, -1);
        break;
    case IT_ADD:
        dumper.Insert(pkey, skey, ts, UNINITIALIZED_EXPIRE_TIME, false, false, isLastNode, autil::StringView(value),
                      regionId, -1);
        break;
    default:
        assert(false);
    }
}

// str: pkey,skey,ts,isLastNode,chunkOffset,inChunkOffset;.....
void KKVValueDumperTest::CheckIterator(const DumpableSKeyNodeIteratorPtr& iter, const string& str)
{
    vector<vector<string>> nodeInfoStrs;
    StringUtil::fromString(str, nodeInfoStrs, ",", ";");
    for (size_t i = 0; i < nodeInfoStrs.size(); i++) {
        assert(nodeInfoStrs[i].size() == 7);
        ASSERT_TRUE(iter->IsValid());
        StringView value;
        uint64_t pkey = iter->Get(value);
        ASSERT_EQ(StringUtil::fromString<uint64_t>(nodeInfoStrs[i][0]), pkey);

        auto nodeSize = sizeof(NormalOnDiskSKeyNode<uint64_t>);
        ASSERT_EQ(nodeSize + 4u, value.size());
        NormalOnDiskSKeyNode<uint64_t>* skeyNode = (NormalOnDiskSKeyNode<uint64_t>*)value.data();

        ASSERT_EQ(StringUtil::fromString<uint64_t>(nodeInfoStrs[i][1]), skeyNode->skey);
        ASSERT_EQ(StringUtil::fromString<uint32_t>(nodeInfoStrs[i][2]), *(uint32_t*)(value.data() + nodeSize));

        OnDiskSKeyOffset offset = skeyNode->offset;
        ASSERT_EQ(nodeInfoStrs[i][3], offset.IsLastNode() ? string("true") : string("false"));
        ASSERT_EQ(StringUtil::fromString<uint64_t>(nodeInfoStrs[i][4]), offset.chunkOffset);
        ASSERT_EQ(StringUtil::fromString<uint64_t>(nodeInfoStrs[i][5]), offset.inChunkOffset);
        ASSERT_EQ(StringUtil::fromString<regionid_t>(nodeInfoStrs[i][6]), iter->GetRegionId());

        iter->MoveToNext();
    }
    ASSERT_FALSE(iter->IsValid());
}

void KKVValueDumperTest::CheckData(const string& str)
{
    vector<vector<string>> dataInfoStrs;
    StringUtil::fromString(str, dataInfoStrs, ":", ";");

    FileReaderPtr fileReader = GET_SEGMENT_DIRECTORY()->CreateFileReader(KKV_VALUE_FILE_NAME, FSOT_MEM);
    size_t totalLen = 0;
    for (size_t i = 0; i < dataInfoStrs.size(); i++) {
        // align to 8 bytes
        totalLen = (totalLen + 7) / 8 * 8;
        assert(dataInfoStrs[i].size() == 2);
        ChunkMeta meta;
        ASSERT_EQ(sizeof(ChunkMeta), fileReader->Read(&meta, sizeof(ChunkMeta), totalLen).GetOrThrow());
        ASSERT_TRUE(meta.isEncoded == 0);
        totalLen += sizeof(ChunkMeta);
        ASSERT_EQ(StringUtil::fromString<uint32_t>(dataInfoStrs[i][0]), meta.length);

        string valueStr;
        valueStr.resize(meta.length);
        ASSERT_EQ((size_t)meta.length,
                  fileReader->Read((void*)valueStr.data(), (size_t)meta.length, totalLen).GetOrThrow());

        size_t cursor = 0;
        vector<string> values;
        StringUtil::fromString(dataInfoStrs[i][1], values, ",");
        for (size_t j = 0; j < values.size(); j++) {
            MultiChar mc(valueStr.c_str() + cursor);
            ASSERT_EQ(values[j].size(), mc.size());
            ASSERT_EQ(values[j], string(mc.data(), mc.size()));
            cursor += mc.length();
        }
        totalLen += meta.length;
    }
    ASSERT_EQ(totalLen, fileReader->GetLength());
}
}} // namespace indexlib::index
