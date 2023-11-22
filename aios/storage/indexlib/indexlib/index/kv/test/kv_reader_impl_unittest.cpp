#include "indexlib/index/kv/test/kv_reader_impl_unittest.h"

#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/test/slow_dump_segment_container.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVReaderImplTest);

KVReaderImplTest::KVReaderImplTest() {}

KVReaderImplTest::~KVReaderImplTest() {}

void KVReaderImplTest::CaseSetUp() { setenv("DISABLE_CODEGEN", "true", 1); }

void KVReaderImplTest::CaseTearDown() { unsetenv("DISABLE_CODEGEN"); }

void KVReaderImplTest::TestGet()
{
    // test read from online segment reader
    // ttl=1, incTimestamp=1, search key=2, searchTs=0. hit online reader
    InnerTestGet("2,1,false,1", "2,2,false,2", 1, 1, 2, 0, true, "2");
    // test read from offline segment reader
    // ttl=1, incTimestamp=1, search key=1, searchTs=0. hit offline reader
    InnerTestGet("1,1,false,1", "2,2,false,2", 1, 1, 1, 0, true, "1");
    // test online value ts < incTimestamp, search offline
    // ttl=1, incTimestamp=2, search key=2, searchTs=0, online value ts=1, hit offline reader
    InnerTestGet("2,1,false,1", "2,2,false,1", 1, 2, 2, 0, true, "1");
    // test online value ts = incTimestamp, use online
    // ttl=1, incTimestamp=1, search key=2, searchTs=0, online value ts=1, hit online reader
    InnerTestGet("2,1,false,1", "2,2,false,1", 1, 1, 2, 0, true, "2");
    // test online isDeleted and doc ts >= inc timestamp
    // ttl=1, incTimestamp=1, search key=2, searchTs=0, online value ts=1, hit online reader
    InnerTestGet("2,1,false,1", "2,2,true,1", 1, 1, 2, 0, false, "");
    // test online isDeleted and doc ts < inc timestamp
    // ttl=1, incTimestamp=2, search key=2, searchTs=0, online value ts=1, hit offline reader
    InnerTestGet("2,1,false,1", "2,2,true,1", 1, 2, 2, 0, true, "1");
    // test online value ts >= inc timestamp but out of time, not search offline index
    // ttl=1, incTimestamp=1, search key=2, searchTs=3, online value ts=1,
    // hit online reader, and out of time
    InnerTestGet("2,1,false,2", "2,2,true,1", 1, 1, 2, 3, false, "");
    // test not exist value
    // search key=3 not exist
    InnerTestGet("2,1,false,2", "2,2,true,2", 1, 1, 3, 0, false, "");
    // test offline value is deleted
    // test offline key=2 is deleted
    InnerTestGet("2,1,true,2", "3,2,true,2", 1, 1, 2, 0, false, "");
    // test offline value outOfTime
    // ttl=1, incTimestamp=1, search key=2, searchTs=4, offline value ts=2,
    // hit offline reader, and out of time
    InnerTestGet("2,1,true,2", "3,2,true,2", 1, 1, 2, 4, false, "");
    // test multi segment for online reader
    InnerTestGet("1,1,false,1;2,2,false,1", "1,11,false,1;2,22,false,1", 0, 1, 1, 0, true, "11");
    // test multi segment for offline reader
    InnerTestGet("1,1,false,1;2,2,false,1", "1,11,false,1;2,22,false,1", 0, 2, 1, 0, true, "1");
}

void KVReaderImplTest::TestMultiInMemSegments()
{
    string field = "key:int32;value:uint64;";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeKVSchema(field, "key", "value");
    IndexPartitionOptions options;
    options.SetIsOnline(true);
    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEMP_DATA_PATH());

    string fullDocs = "cmd=add,key=1,value=1;"
                      "cmd=add,key=2,value=2;";
    pdc.AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,key=3,value=3,ts=0;"
                         "cmd=add,key=4,value=4,ts=0;";
    pdc.AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,key=5,value=5,ts=0;"
                           "cmd=add,key=6,value=6,ts=0;";
    pdc.AddInMemSegment(inMemSegment1);

    string inMemSegment2 = "cmd=add,key=7,value=7,ts=0;"
                           "cmd=add,key=8,value=8,ts=0;"
                           "cmd=add,key=9,value=9,ts=0;";
    pdc.AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=add,key=10,value=10,ts=0;"
                          "cmd=add,key=11,value=11,ts=0;";
    pdc.AddBuildingData(buildingDocs);

    KVIndexConfigPtr kvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    PartitionDataPtr partData = pdc.CreatePartitionData(false);

    KVReaderPtr reader(new KVReaderImpl);
    reader->Open(kvIndexConfig, partData, SearchCachePartitionWrapperPtr(), 0);

    CheckReaderValue(reader, "1", 1);
    CheckReaderValue(reader, "2", 2);
    CheckReaderValue(reader, "3", 3);
    CheckReaderValue(reader, "4", 4);
    CheckReaderValue(reader, "5", 5);
    CheckReaderValue(reader, "6", 6);
    CheckReaderValue(reader, "7", 7);
    CheckReaderValue(reader, "8", 8);
    CheckReaderValue(reader, "9", 9);
    CheckReaderValue(reader, "10", 10);
    CheckReaderValue(reader, "11", 11);
}

void KVReaderImplTest::InnerTestGet(const string& offlineValues, const string& onlineValues, uint64_t ttl,
                                    uint64_t incTimestamp, uint64_t key, uint64_t searchTs, bool success,
                                    const string& expectValue)
{
    KVIndexOptions options;
    options.ttl = ttl;
    options.incTsInSecond = incTimestamp;
    future_lite::executors::SimpleExecutor ex(1);
    {
        KVReaderImpl reader;
        reader.mHasTTL = true;
        reader.mHasSearchCache = false;
        PrepareSegmentReader(offlineValues, onlineValues, reader);
        autil::StringView value;
        bool ret = future_lite::interface::syncAwait(reader.InnerGet(&options, key, value, searchTs, tsc_default), &ex);
        ASSERT_EQ(ret, success);
        if (ret) {
            ASSERT_EQ(expectValue, value.to_string());
        }
    }

    {
        KVReaderImpl reader;
        reader.mHasTTL = false;
        reader.mHasSearchCache = false;
        PrepareSegmentReader(offlineValues, onlineValues, reader);
        autil::StringView value;
        bool ret = future_lite::interface::syncAwait(reader.InnerGet(&options, key, value, searchTs, tsc_default), &ex);
        ASSERT_EQ(ret, success);
        if (ret) {
            ASSERT_EQ(expectValue, value.to_string());
        }
    }
}

// str:"key,value,isdeleted,ts;key,value,isDeleted,ts"
template <typename Reader>
void KVReaderImplTest::PrepareSegmentReader(const std::string& offlineReaderStr, const std::string& onlineReaderStr,
                                            Reader& reader)
{
    vector<vector<string>> values;
    StringUtil::fromString(offlineReaderStr, values, ",", ";");
    // vector<FakeSegmentReader> readers;
    vector<KVSegmentReader> readers;
    for (size_t i = 0; i < values.size(); i++) {
        FakeSegmentReader* segReader = new FakeSegmentReader();
        assert(values[i].size() == 4);
        segReader->SetKeyValue(values[i][0], values[i][1], values[i][2], values[i][3]);
        KVSegmentReader kvSegmentReader;
        kvSegmentReader.mReader.reset((KVSegmentReaderImplBase*)segReader);
        readers.push_back(kvSegmentReader);
    }
    reader.mOfflineSegmentReaders.push_back(readers);

    values.clear();
    readers.clear();
    StringUtil::fromString(onlineReaderStr, values, ",", ";");
    for (size_t i = 0; i < values.size(); i++) {
        // FakeSegmentReader segReader;
        FakeSegmentReader* segReader = new FakeSegmentReader();
        assert(values[i].size() == 4);
        segReader->SetKeyValue(values[i][0], values[i][1], values[i][2], values[i][3]);
        KVSegmentReader kvSegmentReader;
        kvSegmentReader.mReader.reset((KVSegmentReaderImplBase*)segReader);
        reader.mOnlineSegmentReaders.push_back(kvSegmentReader);
    }
    // reader.mOnlineSegmentReaders = readers;
}

void KVReaderImplTest::CheckReaderValue(const KVReaderPtr& reader, const string& key, uint64_t value)
{
    StringView keyStr(key.data(), key.size());
    StringView valueStr;
    future_lite::executors::SimpleExecutor ex(1);
    ASSERT_TRUE(future_lite::interface::syncAwait(reader->GetAsync(keyStr, valueStr), &ex));

    uint64_t actualValue = *(uint64_t*)valueStr.data();
    ASSERT_EQ(value, actualValue);
}
}} // namespace indexlib::index
