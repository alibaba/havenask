#include "indexlib/index/kv/test/kv_doc_reader_unittest.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_doc_reader.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"
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
IE_LOG_SETUP(index, KVDocReaderTest);

KVDocReaderTest::KVDocReaderTest() {}

KVDocReaderTest::~KVDocReaderTest() {}

void KVDocReaderTest::CaseSetUp() { setenv(NEED_STORE_PKEY_VALUE, "true", 1); }

void KVDocReaderTest::CaseTearDown() { unsetenv(NEED_STORE_PKEY_VALUE); }

void KVDocReaderTest::PrepareSchema(string& fields)
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

void KVDocReaderTest::BuildKVTable(string& docString, bool needMerge)
{
    mOptions.GetOfflineConfig().buildConfig.maxDocCount = 2;
    mOptions.GetBuildConfig(false).buildTotalMemory = 1000;
    mOptions.GetBuildConfig(true).buildTotalMemory = 1000;
    mOptions.GetBuildConfig(false).enablePackageFile = false;
    mOptions.GetBuildConfig(true).enablePackageFile = false;
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH()));
    if (needMerge) {
        ASSERT_TRUE(psm.Transfer(BUILD_FULL, docString, "", ""));
    } else {
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    RESET_FILE_SYSTEM();
    mPartitionData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
}

void KVDocReaderTest::CheckResult(map<string, ValueData>& expectedResult)
{
    auto newSchema = SchemaAdapter::LoadAndRewritePartitionSchema(mPartitionData->GetRootDirectory(), mOptions);
    ASSERT_GT(mPartitionData->GetSegmentDatas().size(), 0L);

    KVDocReader kvReader;
    std::string ttlFieldName;
    kvReader.Init(newSchema, mPartitionData, 0, 0, ttlFieldName);
    document::DefaultRawDocument doc;
    uint32_t timestamp = 0;
    uint32_t docCount = 0;
    while (kvReader.Read(&doc, timestamp)) {
        docCount++;
        auto pkey = doc.getField("pkey");
        auto value = doc.getField("value");
        auto iter = expectedResult.find(pkey);
        ASSERT_NE(iter, expectedResult.end());
        ASSERT_EQ(iter->second.value, value);
        ASSERT_EQ(iter->second.timestamp, timestamp);
    }

    ASSERT_EQ(expectedResult.size(), docCount);
}

void KVDocReaderTest::TestKVHelper(string& fields, string& docString, bool needMerge,
                                   map<string, ValueData>& expectedResult)
{
    mSchema = SchemaMaker::MakeKVSchema(fields, "pkey", "value");
    PrepareSchema(fields);
    BuildKVTable(docString, needMerge);
    CheckResult(expectedResult);
}

void KVDocReaderTest::TestNumberHashAndFixedLenKV()
{
    // pkey is number hash, kv is fixed len kv table
    string fields = "pkey:int8;value:int8;";
    string docString = "cmd=delete,pkey=12,value=45,ts=102000000;"
                       "cmd=add,pkey=45,value=67,ts=103000000;"
                       "cmd=add,pkey=35,value=39,ts=104000000;"
                       "cmd=add,pkey=37,value=78,ts=105000000;"
                       "cmd=add,pkey=35,value=12,ts=106000000;"
                       "cmd=add,pkey=45,value=32,ts=107000000;";
    map<string, ValueData> expectedResult;
    expectedResult["45"] = {"32", 107};
    expectedResult["35"] = {"12", 106};
    expectedResult["37"] = {"78", 105};

    TestKVHelper(fields, docString, false, expectedResult);
}

void KVDocReaderTest::TestNumberHashAndVarLenKV()
{
    // pkey is number hash, kv is varlen kv table
    string fields = "pkey:int8;value:string;";
    string docString = "cmd=delete,pkey=12,value=ab,ts=102000000;"
                       "cmd=add,pkey=45,value=cd,ts=103000000;"
                       "cmd=add,pkey=35,value=ef,ts=104000000;"
                       "cmd=add,pkey=37,value=gh,ts=105000000;"
                       "cmd=add,pkey=35,value=ijk,ts=106000000;"
                       "cmd=add,pkey=12,value=ab,ts=107000000;"
                       "cmd=delete,pkey=45,value=rst,ts=108000000;";
    map<string, ValueData> expectedResult;
    expectedResult["12"] = {"ab", 107};
    expectedResult["35"] = {"ijk", 106};
    expectedResult["37"] = {"gh", 105};

    TestKVHelper(fields, docString, false, expectedResult);
}

void KVDocReaderTest::TestKVMerge()
{
    // pkey is not number hash, kv is fixedlen kv table
    string fields = "pkey:string;value:string;";
    string docString = "cmd=add,pkey=pkey1,value=12,ts=102000000;"
                       "cmd=add,pkey=pkey2,value=16,ts=103000000;"
                       "cmd=add,pkey=pkey3,value=17,ts=104000000;"
                       "cmd=add,pkey=pkey4,value=18,ts=105000000;"
                       "cmd=add,pkey=pkey5,value=19,ts=106000000;";
    map<string, ValueData> expectedResult;
    expectedResult["pkey1"] = {"12", 102};
    expectedResult["pkey2"] = {"16", 103};
    expectedResult["pkey3"] = {"17", 104};
    expectedResult["pkey4"] = {"18", 105};
    expectedResult["pkey5"] = {"19", 106};

    TestKVHelper(fields, docString, true, expectedResult);
}

void KVDocReaderTest::TestNotNumberHashAndFixedLenKV()
{
    // pkey is not number hash, kv is fixedlen kv table
    string fields = "pkey:string;value:int8;";
    string docString = "cmd=add,pkey=pkey1,value=12,ts=102000000;"
                       "cmd=add,pkey=pkey1,value=13,ts=103000000;"
                       "cmd=add,pkey=pkey1,value=14,ts=104000000;"
                       "cmd=delete,pkey=pkey4,value=15,ts=105000000;"
                       "cmd=add,pkey=pkey2,value=16,ts=106000000;"
                       "cmd=add,pkey=pkey3,value=17,ts=107000000;"
                       "cmd=add,pkey=pkey1,value=18,ts=108000000;"
                       "cmd=add,pkey=pkey4,value=19,ts=109000000;"
                       "cmd=delete,pkey=pkey3,value=20,ts=110000000;";

    map<string, ValueData> expectedResult;
    expectedResult["pkey1"] = {"18", 108};
    expectedResult["pkey4"] = {"19", 109};
    expectedResult["pkey2"] = {"16", 106};

    TestKVHelper(fields, docString, false, expectedResult);
}

void KVDocReaderTest::TestNotNumberHashAndVarLenKV()
{
    // pkey is not number hash, kv is varlen kv table
    string fields = "pkey:string;value:string;";
    string docString = "cmd=add,pkey=pkey1,value=abcd,ts=102000000;"
                       "cmd=add,pkey=pkey1,value=rst,ts=103000000;"
                       "cmd=add,pkey=pkey1,value=ijk,ts=104000000;"
                       "cmd=delete,pkey=pkey4,value=opt,ts=105000000;"
                       "cmd=add,pkey=pkey2,value=xyz,ts=106000000;"
                       "cmd=add,pkey=pkey3,value=abcd,ts=107000000;"
                       "cmd=add,pkey=pkey1,value=wert,ts=108000000;"
                       "cmd=add,pkey=pkey4,value=qert,ts=109000000;"
                       "cmd=delete,pkey=pkey3,value=port,ts=110000000";
    map<string, ValueData> expectedResult;
    expectedResult["pkey1"] = {"wert", 108};
    expectedResult["pkey4"] = {"qert", 109};
    expectedResult["pkey2"] = {"xyz", 106};

    TestKVHelper(fields, docString, false, expectedResult);
}

void KVDocReaderTest::TestSimpleValue()
{
    // value is simple
    string fields = "pkey:string;value:int8";
    string docString = "cmd=add,pkey=pk1,value=-1,ts=102000000;"
                       "cmd=add,pkey=pk2,value=-2,ts=103000000;"
                       "cmd=add,pkey=pk4,value=-2,ts=104000000;"
                       "cmd=add,pkey=pk3,value=-3,ts=105000000;"
                       "cmd=delete,pkey=pk4,ts=5000000";
    map<string, ValueData> expectedResult;
    expectedResult["pk1"] = {"-1", 102};
    expectedResult["pk2"] = {"-2", 103};
    expectedResult["pk3"] = {"-3", 105};

    TestKVHelper(fields, docString, false, expectedResult);
}

void KVDocReaderTest::TestPackValue()
{
    // value is packed
    InnerTestPackValue("");
    InnerTestPackValue("impact");
    InnerTestPackValue("plain");
}

void KVDocReaderTest::InnerTestPackValue(const string& valueFormat)
{
    fslib::fs::FileSystem::remove(GET_TEMP_DATA_PATH());
    string fields = "strpk:string;longval:uint64;strval:string;int8val:int8;uint8val:uint8";
    mSchema = SchemaMaker::MakeKVSchema(fields, "strpk", "int8val;uint8val;strpk;longval;strval");
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
    indexPreference.SetValueParam(valueParam);
    mKVConfig->SetIndexPreference(indexPreference);

    string docString = "cmd=add,strpk=pk1,longval=1,strval=val1,int8val=-1,uint8val=1,ts=1000000;"
                       "cmd=add,strpk=pk2,longval=2,strval=val2,int8val=-2,uint8val=2,ts=2000000;"
                       "cmd=add,strpk=pk4,longval=4,strval=val4,int8val=-4,uint8val=4,ts=4000000;"
                       "cmd=add,strpk=pk1,longval=4,strval=val1_2,int8val=-11,uint8val=11,ts=4000000"
                       "cmd=add,strpk=pk3,longval=3,strval=val3,int8val=-3,uint8val=3,ts=3000000;"
                       "cmd=delete,strpk=pk4,ts=5000000";
    BuildKVTable(docString, false);

#define CHECK_RAW_DOC(ts, int8val, uint8val, longval, strval)                                                          \
    {                                                                                                                  \
        DefaultRawDocument rawDoc;                                                                                     \
        uint32_t timestamp;                                                                                            \
        INDEXLIB_TEST_TRUE(!kvDocReader.IsEof());                                                                      \
        INDEXLIB_TEST_TRUE(kvDocReader.Read(&rawDoc, timestamp));                                                      \
        ASSERT_EQ(StringView(int8val), rawDoc.getField(StringView("int8val")));                                        \
        ASSERT_EQ(StringView(uint8val), rawDoc.getField(StringView("uint8val")));                                      \
        ASSERT_EQ(StringView(longval), rawDoc.getField(StringView("longval")));                                        \
        ASSERT_EQ(StringView(strval), rawDoc.getField(StringView("strval")));                                          \
        ASSERT_EQ(ts, timestamp);                                                                                      \
    }

    {
        auto partData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
        KVDocReader kvDocReader;
        kvDocReader.Init(mSchema, partData, 0, 0, "");
        CHECK_RAW_DOC(3, "-3", "3", "3", "val3");
        CHECK_RAW_DOC(1, "-1", "1", "1", "val1");
        CHECK_RAW_DOC(2, "-2", "2", "2", "val2");
    }

#undef CHECK_RAW_DOC
}

// TODO(xinfei.sxf) test eof / seek / timestamp

}} // namespace indexlib::index
