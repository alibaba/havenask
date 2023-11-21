#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "indexlib/index/kv/test/VarLenIndexerReadWriteTestBase.h"

namespace indexlibv2::index {

class VarLenValueOffsetReclaimTest : public VarLenIndexerReadWriteTestBase
{
protected:
    void MakeSchema(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames) override
    {
        KVIndexConfigBuilder builder(fieldNames, keyName, valueNames);
        std::vector<std::string> valueFormats = {"", "impact", "plain"};
        size_t idx = GET_CLASS_NAME().size() % 3;
        builder.SetValueFormat(valueFormats[idx]);
        builder.SetTTL(0);
        ASSERT_TRUE(builder.Valid());
        auto result = builder.Finalize();
        _schema = std::move(result.first);
        _indexConfig = std::move(result.second);
        _enableMemoryReclaim = true;
        auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
        ASSERT_TRUE(typeId.isVarLen);
        ASSERT_TRUE(typeId.shortOffset);
        ASSERT_FALSE(typeId.fileCompress);
        ASSERT_TRUE(typeId.hasTTL);
    }

    template <FieldType ft>
    void DoTestValueOffsetReclaim()
    {
        auto f2TypeStr = indexlibv2::config::FieldConfig::FieldTypeToStr(ft);
        std::string fields = "key:uint32;f1:int64;f2:" + std::string(f2TypeStr);
        MakeSchema(fields, "key", "f1;f2");

        int reclaimFreq = 0;
        auto memReclaimer = std::make_shared<indexlibv2::framework::EpochBasedMemReclaimer>(reclaimFreq, nullptr);

        auto indexer = std::make_unique<VarLenKVMemIndexer>(
            true, DEFAULT_MEMORY_USE_IN_BYTES, VarLenKVMemIndexer::DEFAULT_KEY_VALUE_MEM_RATIO,
            VarLenKVMemIndexer::DEFAULT_VALUE_COMPRESSION_RATIO, memReclaimer.get());
        ASSERT_TRUE(indexer->Init(_indexConfig, nullptr).IsOK());

        auto reader = indexer->CreateInMemoryReader();
        ASSERT_NE(nullptr, reader);

        using T = typename indexlib::index::FieldTypeTraits<ft>::AttrItemType;
        PackAttributeFormatter formatter;
        auto [status, packAttrConfig] = _indexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(formatter.Init(packAttrConfig));
        auto* ref1 = formatter.GetAttributeReferenceTyped<int64_t>("f1");
        ASSERT_NE(nullptr, ref1);
        auto* ref2 = formatter.GetAttributeReferenceTyped<T>("f2");
        ASSERT_NE(nullptr, ref2);

        std::vector<std::string> cmds = {"add", "add",    "add", "add", "add",   "delete",
                                         "add", "delete", "add", "add", "delete"};
        std::vector<std::string> keys = {"1", "1", "2", "1", "3", "2", "3", "3", "3", "2", "2"};
        std::vector<std::string> f1s = {"101", "102", "222", "111", "333", "", "333", "", "333", "222", ""};
        std::vector<std::string> f2s = {"1", "1", "22", "22", "333", "", "4444", "", "4444", "55555", ""};
        std::vector<std::string> tss = {"101000000", "101000000", "102000000", "101000000", "103000000", "104000000",
                                        "103000000", "103000000", "103000000", "102000000", "104000000"};
        ASSERT_EQ(cmds.size(), keys.size());
        ASSERT_EQ(cmds.size(), f1s.size());
        ASSERT_EQ(cmds.size(), f2s.size());
        ASSERT_EQ(cmds.size(), tss.size());

        std::string docStr;
        for (size_t i = 0; i < cmds.size(); ++i) {
            docStr += "cmd=" + cmds[i] + ",key=" + keys[i] + ",f1=" + f1s[i] + ",f2=" + f2s[i] + ",ts=" + tss[i] + ";";
        }
        std::map<int32_t, size_t> freeValueLen2CountMap;
        std::map<uint64_t, int32_t> key2CurValueLen;

        auto rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        ASSERT_EQ(cmds.size(), rawDocs.size());
        size_t curValueMemoryUse = 0ul;
        autil::mem_pool::Pool pool;
        for (size_t i = 0; i < rawDocs.size(); ++i) {
            auto& rawDoc = rawDocs[i];
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto s = indexer->Build(docBatch.get());
            ASSERT_TRUE(s.IsOK()) << s.ToString();
            memReclaimer->IncreaseEpoch();
            memReclaimer->IncreaseEpoch();
            memReclaimer->TryReclaim();

            auto metrics = std::make_shared<indexlib::framework::SegmentMetrics>();
            indexer->FillStatistics(metrics);
            auto groupMetrics = metrics->GetSegmentGroupMetrics("key");
            ASSERT_NE(nullptr, groupMetrics);
            size_t memoryUse = groupMetrics->Get<size_t>(SegmentStatistics::KV_VALUE_MEM_USE);

            size_t valueLen = sizeof(int64_t);
            valueLen += (ft == FieldType::ft_string) ? f2s[i].size() : sizeof(int64_t);
            if (cmds[i] == "add") {
                if (freeValueLen2CountMap[valueLen] > 0) {
                    ASSERT_EQ(curValueMemoryUse, memoryUse) << i;
                    --freeValueLen2CountMap[valueLen];
                } else {
                    ASSERT_LT(curValueMemoryUse, memoryUse) << i;
                }
            } else {
                ASSERT_EQ(curValueMemoryUse, memoryUse) << i;
            }
            curValueMemoryUse = memoryUse;

            uint64_t key = 0l;
            ASSERT_TRUE(autil::StringUtil::strToUInt64(keys[i].c_str(), key));
            auto iterator = key2CurValueLen.find(key);
            if (iterator != key2CurValueLen.end()) {
                ++freeValueLen2CountMap[iterator->second];
                key2CurValueLen.erase(iterator);
            }
            if (cmds[i] == "add") {
                key2CurValueLen[key] = valueLen;
            }

            autil::StringView value;
            uint64_t ts = 0ul;

            uint64_t expectTs = 0l;
            ASSERT_TRUE(autil::StringUtil::strToUInt64(tss[i].c_str(), expectTs));
            auto status = reader->Get(key, value, ts, &pool, nullptr, nullptr);
            if (cmds[i] != "add") {
                ASSERT_EQ(indexlib::util::Status::DELETED, status) << i;
                ASSERT_EQ(ts * 1000000, expectTs) << i;
                continue;
            }

            ASSERT_EQ(indexlib::util::Status::OK, status) << i;
            ASSERT_EQ(ts * 1000000, expectTs) << i;

            int64_t f1Value = 0ul;
            ASSERT_TRUE(ref1->GetValue(value.data(), f1Value));
            int64_t expectedF1Value = 0ul;
            ASSERT_TRUE(autil::StringUtil::strToInt64(f1s[i].c_str(), expectedF1Value));
            ASSERT_EQ(expectedF1Value, f1Value);

            if constexpr (std::is_same<int64_t, T>::value) {
                int64_t f2Value = 0ul;
                ASSERT_TRUE(ref2->GetValue(value.data(), f2Value));
                int64_t expectedF2Value = 0ul;
                ASSERT_TRUE(autil::StringUtil::strToInt64(f2s[i].c_str(), expectedF2Value));
                ASSERT_EQ(expectedF2Value, f2Value);
            } else {
                autil::MultiChar f2Value;
                ASSERT_TRUE(ref2->GetValue(value.data(), f2Value));
                ASSERT_EQ(std::string(f2Value.data(), f2Value.size()), f2s[i]);
            }
        }
    }
};

TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadInt8) { DoTestBuildAndRead<ft_int8>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadInt16) { DoTestBuildAndRead<ft_int16>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadInt32) { DoTestBuildAndRead<ft_int32>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadInt64) { DoTestBuildAndRead<ft_int64>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadUInt8) { DoTestBuildAndRead<ft_uint8>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadUInt16) { DoTestBuildAndRead<ft_uint16>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadUInt32) { DoTestBuildAndRead<ft_uint32>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadUInt64) { DoTestBuildAndRead<ft_uint64>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadFloat) { DoTestBuildAndRead<ft_float>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadDouble) { DoTestBuildAndRead<ft_double>(); }
TEST_F(VarLenValueOffsetReclaimTest, testBuildAndReadString) { DoTestBuildAndRead<ft_string>(true); }
TEST_F(VarLenValueOffsetReclaimTest, DoTestValueOffsetReclaimFixedLen) { DoTestValueOffsetReclaim<ft_int64>(); }
TEST_F(VarLenValueOffsetReclaimTest, DoTestValueOffsetReclaimVarLen) { DoTestValueOffsetReclaim<ft_string>(); }

} // namespace indexlibv2::index
