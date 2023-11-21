#include "indexlib/index/kkv/test/kkv_doc_reader_unittest.h"

#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/index/kkv/kkv_doc_reader.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KKVDocReaderTest);

KKVDocReaderTest::KKVDocReaderTest() {}

KKVDocReaderTest::~KKVDocReaderTest() {}

void KKVDocReaderTest::CaseSetUp() { setenv(NEED_STORE_PKEY_VALUE, "true", 1); }

void KKVDocReaderTest::CaseTearDown() { unsetenv(NEED_STORE_PKEY_VALUE); }

void KKVDocReaderTest::PrepareSchema(const string& fields)
{
    mSchema->SetEnableTTL(true);
    mKVConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    mKVConfig->SetTTL(10000);
    KVIndexPreference& kvIndexPreference = mKVConfig->GetIndexPreference();
    KVIndexPreference::ValueParam valueParam = kvIndexPreference.GetValueParam();
    valueParam.EnableFixLenAutoInline();
    kvIndexPreference.SetValueParam(valueParam);
    ASSERT_TRUE(mKVConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline());
}

void KKVDocReaderTest::BuildKKVTable(const string& docString)
{
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetBuildConfig(false).buildTotalMemory = 2000; // 20MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 2000;  // 20MB
    mOptions.GetBuildConfig(false).enablePackageFile = false;
    mOptions.GetBuildConfig(true).enablePackageFile = false;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    RESET_FILE_SYSTEM();
    mPartitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
}

void KKVDocReaderTest::CheckKKVResult(const map<KeyData, ValueData>& expectedResult)
{
    auto newSchema = SchemaAdapter::LoadAndRewritePartitionSchema(mPartitionData->GetRootDirectory(), mOptions);

    auto iter = mPartitionData->Begin();
    size_t segmentCount = 0;
    while (iter != mPartitionData->End()) {
        iter++;
        segmentCount++;
    }
    ASSERT_GT(segmentCount, 1);

    KKVDocReader kkvReader;
    std::string ttlFieldName;
    kkvReader.Init(newSchema, mPartitionData, 0, 0, ttlFieldName);
    document::DefaultRawDocument doc;
    uint32_t timestamp = 0;
    uint32_t docCount = 0;
    while (kkvReader.Read(&doc, timestamp)) {
        docCount++;
        auto pkey = doc.getField("pkey");
        auto skey = doc.getField("skey");
        auto value = doc.getField("value");
        auto iter = expectedResult.find({pkey, skey});
        ASSERT_NE(iter, expectedResult.end());
        ASSERT_EQ(iter->second.value1, value);
        ASSERT_EQ(iter->second.timestamp, timestamp);
    }
    ASSERT_FALSE(kkvReader.Read(&doc, timestamp));
    ASSERT_TRUE(kkvReader.IsEof());
    ASSERT_EQ(expectedResult.size(), docCount);
}

void KKVDocReaderTest::TestKKVHelper(const string& fields, const string& docString,
                                     const map<KeyData, ValueData>& expectedResult)
{
    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "value");
    PrepareSchema(fields);
    BuildKKVTable(docString);
    CheckKKVResult(expectedResult);
}

void KKVDocReaderTest::TestNumberHash()
{
    // pkey is number hash
    string fields = "pkey:int8;skey:int32;value:string;";
    string docString = "cmd=add,pkey=37,skey=12,value=21,ts=102000000;"
                       "cmd=add,pkey=37,skey=13,value=24,ts=103000000;"
                       "cmd=add,pkey=38,skey=13,value=25,ts=104000000;";

    map<KeyData, ValueData> expectedResult;
    expectedResult[KeyData {"37", "12"}] = {"21", "", 102};
    expectedResult[KeyData {"37", "13"}] = {"24", "", 103};
    expectedResult[KeyData {"38", "13"}] = {"25", "", 104};

    TestKKVHelper(fields, docString, expectedResult);
}

void KKVDocReaderTest::TestNotNumberHash()
{
    // pkey is not number hash
    string fields = "pkey:string;skey:int32;value:string;";

    string docString = "cmd=add,pkey=pkey1,skey=1,value=abcd,ts=102000000;"
                       "cmd=add,pkey=pkey1,skey=1,value=rst,ts=103000000;"
                       "cmd=add,pkey=pkey1,skey=3,value=opq,ts=104000000;"
                       "cmd=delete,pkey=pkey4,skey=7,value=rst,ts=105000000;"
                       "cmd=add,pkey=pkey2,skey=5,value=wrt,ts=106000000;"
                       "cmd=add,pkey=pkey3,skey=8,value=xyz,ts=107000000;"
                       "cmd=add,pkey=pkey1,skey=4,value=pot,ts=108000000;"
                       "cmd=add,pkey=pkey4,skey=7,value=rdma,ts=109000000;"
                       "cmd=delete,pkey=pkey3,skey=8,value=pty,ts=110000000";

    map<KeyData, ValueData> expectedResult;
    expectedResult[KeyData {"pkey1", "1"}] = {"rst", "", 103};
    expectedResult[KeyData {"pkey1", "3"}] = {"opq", "", 104};
    expectedResult[KeyData {"pkey1", "4"}] = {"pot", "", 108};
    expectedResult[KeyData {"pkey4", "7"}] = {"rdma", "", 109};
    expectedResult[KeyData {"pkey2", "5"}] = {"wrt", "", 106};

    TestKKVHelper(fields, docString, expectedResult);
}

void KKVDocReaderTest::CheckKKVResultForMultiField(const map<KeyData, ValueData>& expectedResult)
{
    auto newSchema = SchemaAdapter::LoadAndRewritePartitionSchema(mPartitionData->GetRootDirectory(), mOptions);
    ASSERT_GT(mPartitionData->GetSegmentDatas().size(), 1L);

    KKVDocReader kkvReader;
    std::string ttlFieldName;
    kkvReader.Init(newSchema, mPartitionData, 0, 0, ttlFieldName);
    document::DefaultRawDocument doc;
    uint32_t timestamp = 0;
    uint32_t docCount = 0;
    while (kkvReader.Read(&doc, timestamp)) {
        docCount++;
        auto pkey = doc.getField("pkey");
        auto skey = doc.getField("skey");
        auto value1 = doc.getField("value1");
        auto value2 = doc.getField("value2");
        auto iter = expectedResult.find({pkey, skey});
        ASSERT_NE(iter, expectedResult.end());
        ASSERT_EQ(iter->second.value1, value1);
        ASSERT_EQ(iter->second.value2, value2);
        ASSERT_EQ(iter->second.timestamp, timestamp);
    }
    ASSERT_FALSE(kkvReader.Read(&doc, timestamp));
    ASSERT_TRUE(kkvReader.IsEof());
    ASSERT_EQ(expectedResult.size(), docCount);
}

void KKVDocReaderTest::InnerTestReadValue(const string& fields, bool optimizedStoreSKey, const string& valueFormat)
{
    fslib::fs::FileSystem::remove(GET_TEMP_DATA_PATH());

    mSchema = SchemaMaker::MakeKKVSchema(fields, "pkey", "skey", "skey;value1;value2");
    PrepareSchema(fields);

    string indexRoot = GET_TEMP_DATA_PATH();
    KVIndexPreference indexPreference = mKVConfig->GetIndexPreference();
    KVIndexPreference::ValueParam valueParam = indexPreference.GetValueParam();
    if (valueFormat == "plain") {
        valueParam.EnablePlainFormat(true);
        indexRoot += "/plain";
    } else if (valueFormat == "impact") {
        valueParam.EnableValueImpact(true);
        indexRoot += "/impact";
    }
    mKVConfig->SetIndexPreference(indexPreference);

    string docString = "cmd=add,pkey=1,skey=1,value1=1,value2=abc,ts=102000000;"
                       "cmd=add,pkey=2,skey=1,value1=2,value2=bcd,ts=103000000;"
                       "cmd=add,pkey=2,skey=2,value1=3,value2=est,ts=104000000;"
                       "cmd=add,pkey=1,skey=2,value1=4,value2=opt,ts=105000000;"
                       "cmd=add,pkey=4,sskeyk=1,value1=8,value2=isrt,ts=106000000;"
                       "cmd=add,pkey=3,skey=1,value1=5,value2=rest,ts=107000000;"
                       "cmd=delete,pkey=4,ts=108000000;"
                       "cmd=add,pkey=2,skey=2,value1=6,value2=xyz,ts=109000000;"
                       "cmd=add,pkey=2,skey=3,value1=7,value2=rdma,ts=110000000";

    map<KeyData, ValueData> expectedResult;
    expectedResult[KeyData {"1", "1"}] = {"1", "abc", 102};
    expectedResult[KeyData {"1", "2"}] = {"4", "opt", 105};
    expectedResult[KeyData {"2", "1"}] = {"2", "bcd", 103};
    expectedResult[KeyData {"2", "2"}] = {"6", "xyz", 109};
    expectedResult[KeyData {"2", "3"}] = {"7", "rdma", 110};
    expectedResult[KeyData {"3", "1"}] = {"5", "rest", 107};

    BuildKKVTable(docString);
    CheckKKVResultForMultiField(expectedResult);
}

void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyUInt8()
{
    InnerTestReadValue("pkey:uint32;skey:uint8;value1:uint32;value2:string", true, "impact");
}
void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyUInt16()
{
    InnerTestReadValue("pkey:uint32;skey:uint16;value1:uint32;value2:string", true, "plain");
}
void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyUInt32()
{
    InnerTestReadValue("pkey:uint32;skey:uint32;value1:uint32;value2:string", true, "");
}
void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyUInt64()
{
    InnerTestReadValue("pkey:uint32;skey:uint64;value1:uint32;value2:string", true, "plain");
}
void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyInt8()
{
    InnerTestReadValue("pkey:uint32;skey:int8;value1:uint32;value2:string", true, "impact");
}
void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyInt16()
{
    InnerTestReadValue("pkey:uint32;skey:int16;value1:uint32;value2:string", true, "");
}
void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyInt32()
{
    InnerTestReadValue("pkey:uint32;skey:int32;value1:uint32;value2:string", true, "");
}
void KKVDocReaderTest::TestReadWithOptimizedStoreSKeyInt64()
{
    InnerTestReadValue("pkey:uint32;skey:int64;value1:uint32;value2:string", true, "plain");
}

void KKVDocReaderTest::TestReadWithNotOptimizedStoreFloatSKey()
{
    InnerTestReadValue("pkey:uint32;skey:float;value1:uint32;value2:string", false, "plain");
}

void KKVDocReaderTest::TestReadWithNotOptimizedStoreDoubleSKey()
{
    InnerTestReadValue("pkey:uint32;skey:double;value1:uint32;value2:string", false, "impact");
}

void KKVDocReaderTest::TestReadWithNotOptimizedStoreStringSKey()
{
    InnerTestReadValue("pkey:uint32;skey:string;value1:uint32;value2:string", false, "");
}

}} // namespace indexlib::index
