#include "indexlib/index/kv/KVSortDataCollector.h"

#include "indexlib/document/raw_document/test/RawDocumentMaker.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/index/common/data_structure/ExpandableValueAccessor.h"
#include "indexlib/index/kv/InMemoryValueWriter.h"
#include "indexlib/index/kv/VarLenKVMemIndexer.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlibv2::index {

class KVSortDataCollectorTest : public TESTBASE
{
protected:
    void PrepareCollector(KVSortDataCollector& collector, const config::SortDescriptions& sortDescriptions,
                          bool fixedLen)
    {
        std::string docStr;
        std::vector<std::shared_ptr<document::RawDocument>> rawDocs;
        _indexer.reset(new VarLenKVMemIndexer(DEFAULT_MEMORY_USE_IN_BYTES));

        if (!fixedLen) {
            std::tie(_schema, _indexConfig) = KVIndexConfigBuilder::MakeIndexConfig(
                "key:uint32;value1:string;value2:long", "key", "key;value1;value2");
            ASSERT_TRUE(collector.Init(_indexConfig, sortDescriptions));
            ASSERT_TRUE(_indexer->Init(_indexConfig, nullptr).IsOK());
            docStr = "cmd=add,key=1,value1=abc,value2=1,ts=101000000;"
                     "cmd=add,key=2,value1=bcd,value2=2,ts=102000000;"
                     "cmd=add,key=4,value1=def,value2=3,ts=102000000;"
                     "cmd=add,key=3,value1=cde,value2=3,ts=103000000;"
                     "cmd=delete,key=2,ts=104000000;";
            rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        } else {
            std::tie(_schema, _indexConfig) = KVIndexConfigBuilder::MakeIndexConfig(
                "key:uint32;value1:float;value2:long", "key", "key;value1;value2");
            _indexConfig->GetValueConfig()->EnableCompactFormat(true);
            ASSERT_TRUE(collector.Init(_indexConfig, sortDescriptions));
            ASSERT_TRUE(_indexer->Init(_indexConfig, nullptr).IsOK());
            docStr = "cmd=add,key=1,value1=1.0,value2=1,ts=101000000;"
                     "cmd=add,key=2,value1=2.0,value2=2,ts=102000000;"
                     "cmd=add,key=4,value1=3.0,value2=3,ts=102000000;"
                     "cmd=add,key=3,value1=4.0,value2=3,ts=103000000;"
                     "cmd=delete,key=5,ts=104000000;";
            rawDocs = document::RawDocumentMaker::MakeBatch(docStr);
        }
        vector<uint32_t> keys = {1u, 2u, 4u, 3u, 5u};
        size_t pos = 0;
        for (const auto& rawDoc : rawDocs) {
            auto docBatch = document::KVDocumentBatchMaker::Make(_schema, {rawDoc});
            ASSERT_TRUE(docBatch);
            auto offset = _indexer->TEST_GetValueWriter()->GetLength();
            auto s = _indexer->Build(docBatch.get());
            ASSERT_TRUE(s.IsOK()) << s.ToString();
            Record record;
            record.key = keys[pos];
            if (4 == pos) {
                record.deleted = true;
            }
            pos++;
            auto valueAccessor = _indexer->TEST_GetValueWriter()->GetValueAccessor();
            if (!record.deleted) {
                record.value = StringView(valueAccessor->GetValue(offset), 0);
            }
            collector.Collect(record);
        }
    }
    std::string GetRecordAttributeStrValue(const std::unique_ptr<RecordComparator>& recordComparator,
                                           AttributeReference* ref, Record& record)
    {
        std::string value;
        autil::StringView recordValue = recordComparator->TEST_GetValueComparator()->DecodeValue(record.value);
        ref->GetStrValue(recordValue.data(), value, &_pool);
        return value;
    }

protected:
    autil::mem_pool::Pool _pool;
    shared_ptr<config::ITabletSchema> _schema;
    shared_ptr<indexlibv2::config::KVIndexConfig> _indexConfig;
    shared_ptr<VarLenKVMemIndexer> _indexer;

protected:
    static constexpr int64_t DEFAULT_MEMORY_USE_IN_BYTES = 1024 * 1024;
};

TEST_F(KVSortDataCollectorTest, testGetSortFieldValue)
{
    // test get value by one field in not fixed len value
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription("value2", config::sp_asc);
        sortDescriptions.push_back(sortDescription);
        PrepareCollector(collector, sortDescriptions, /*fixedLen=*/false);
        const auto& recordComparator = collector.TEST_GetRecordComparator();
        auto valueComparator = recordComparator->TEST_GetValueComparator();
        ASSERT_EQ(int32_t(-1), valueComparator->TEST_GetFixedValueLen());
        auto ref = valueComparator->TEST_GetSortComps()[0]->TEST_GetAttributeReference();
        auto& records = collector.TEST_GetRecords();
        ASSERT_EQ(size_t(5), records.size());
        ASSERT_EQ("1", GetRecordAttributeStrValue(recordComparator, ref, records[0]));
        ASSERT_EQ("2", GetRecordAttributeStrValue(recordComparator, ref, records[1]));
        ASSERT_EQ("3", GetRecordAttributeStrValue(recordComparator, ref, records[2]));
        ASSERT_EQ("3", GetRecordAttributeStrValue(recordComparator, ref, records[3]));
        ASSERT_TRUE(records[4].deleted);
    }
    // test get value by two field in fixed len value
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription1("value1", config::sp_asc);
        sortDescriptions.push_back(sortDescription1);
        config::SortDescription sortDescription2("value2", config::sp_asc);
        sortDescriptions.push_back(sortDescription2);
        PrepareCollector(collector, sortDescriptions, /*fixedLen=*/true);
        const auto& recordComparator = collector.TEST_GetRecordComparator();
        auto valueComparator = recordComparator->TEST_GetValueComparator();
        ASSERT_NE(int32_t(-1), valueComparator->TEST_GetFixedValueLen());
        auto ref1 = valueComparator->TEST_GetSortComps()[0]->TEST_GetAttributeReference();
        auto ref2 = valueComparator->TEST_GetSortComps()[1]->TEST_GetAttributeReference();
        auto& records = collector.TEST_GetRecords();
        ASSERT_EQ(size_t(5), records.size());
        ASSERT_EQ("1", GetRecordAttributeStrValue(recordComparator, ref1, records[0]));
        ASSERT_EQ("2", GetRecordAttributeStrValue(recordComparator, ref1, records[1]));
        ASSERT_EQ("3", GetRecordAttributeStrValue(recordComparator, ref1, records[2]));
        ASSERT_EQ("4", GetRecordAttributeStrValue(recordComparator, ref1, records[3]));
        ASSERT_TRUE(records[4].deleted);

        ASSERT_EQ("1", GetRecordAttributeStrValue(recordComparator, ref2, records[0]));
        ASSERT_EQ("2", GetRecordAttributeStrValue(recordComparator, ref2, records[1]));
        ASSERT_EQ("3", GetRecordAttributeStrValue(recordComparator, ref2, records[2]));
        ASSERT_EQ("3", GetRecordAttributeStrValue(recordComparator, ref2, records[3]));
        ASSERT_TRUE(records[4].deleted);
    }
}

TEST_F(KVSortDataCollectorTest, testCollectAndSort)
{
    // test sort by one field in fixed len value
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription("value2", config::sp_asc);
        sortDescriptions.push_back(sortDescription);
        PrepareCollector(collector, sortDescriptions, /*fixedLen=*/true);
        ASSERT_EQ(keytype_t(5), collector.Size());
        collector.Sort();
        auto& records = collector.TEST_GetRecords();
        ASSERT_EQ(keytype_t(5), records[0].key);
        ASSERT_TRUE(records[0].deleted);
        ASSERT_EQ(keytype_t(1), records[1].key);
        ASSERT_FALSE(records[1].deleted);
        ASSERT_EQ(keytype_t(2), records[2].key);
        ASSERT_FALSE(records[2].deleted);
        ASSERT_EQ(keytype_t(3), records[3].key);
        ASSERT_FALSE(records[3].deleted);
        ASSERT_EQ(keytype_t(4), records[4].key);
        ASSERT_FALSE(records[4].deleted);
    }
    // test sort by one field in not fixed len value
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription("value2", config::sp_desc);
        sortDescriptions.push_back(sortDescription);
        PrepareCollector(collector, sortDescriptions, /*fixedLen=*/false);
        ASSERT_EQ(keytype_t(5), collector.Size());
        collector.Sort();
        auto& records = collector.TEST_GetRecords();
        ASSERT_EQ(keytype_t(5), records[0].key);
        ASSERT_TRUE(records[0].deleted);
        ASSERT_EQ(keytype_t(3), records[1].key);
        ASSERT_FALSE(records[1].deleted);
        ASSERT_EQ(keytype_t(4), records[2].key);
        ASSERT_FALSE(records[2].deleted);
        ASSERT_EQ(keytype_t(2), records[3].key);
        ASSERT_FALSE(records[3].deleted);
        ASSERT_EQ(keytype_t(1), records[4].key);
        ASSERT_FALSE(records[4].deleted);
    }
    // test sort by two field in fixed len value
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription1("value2", config::sp_desc);
        sortDescriptions.push_back(sortDescription1);
        config::SortDescription sortDescription2("key", config::sp_desc);
        sortDescriptions.push_back(sortDescription2);
        PrepareCollector(collector, sortDescriptions, /*fixedLen=*/true);
        ASSERT_EQ(keytype_t(5), collector.Size());
        collector.Sort();
        auto& records = collector.TEST_GetRecords();
        ASSERT_EQ(keytype_t(5), records[0].key);
        ASSERT_TRUE(records[0].deleted);
        ASSERT_EQ(keytype_t(4), records[1].key);
        ASSERT_FALSE(records[1].deleted);
        ASSERT_EQ(keytype_t(3), records[2].key);
        ASSERT_FALSE(records[2].deleted);
        ASSERT_EQ(keytype_t(2), records[3].key);
        ASSERT_FALSE(records[3].deleted);
        ASSERT_EQ(keytype_t(1), records[4].key);
        ASSERT_FALSE(records[4].deleted);
    }
}

TEST_F(KVSortDataCollectorTest, testInitFailed)
{
    // test sort by multi field
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription1("value1", config::sp_desc);
        sortDescriptions.push_back(sortDescription1);
        auto [_schema, _indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("key:uint32;value1:int32:true;value2:long",
                                                                             "key", "key;value1;value2");
        ASSERT_FALSE(collector.Init(_indexConfig, sortDescriptions));
    }
    // test sort by string field
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription1("value1", config::sp_desc);
        sortDescriptions.push_back(sortDescription1);
        auto [_schema, _indexConfig] =
            KVIndexConfigBuilder::MakeIndexConfig("key:uint32;value1:string;value2:long", "key", "key;value1;value2");
        ASSERT_FALSE(collector.Init(_indexConfig, sortDescriptions));
    }
    // test not existed field
    {
        KVSortDataCollector collector;
        config::SortDescriptions sortDescriptions;
        config::SortDescription sortDescription1("not_existed_field", config::sp_desc);
        sortDescriptions.push_back(sortDescription1);
        auto [_schema, _indexConfig] =
            KVIndexConfigBuilder::MakeIndexConfig("key:uint32;value1:string;value2:long", "key", "key;value1;value2");
        ASSERT_FALSE(collector.Init(_indexConfig, sortDescriptions));
    }
}

} // namespace indexlibv2::index
