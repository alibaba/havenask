#include "indexlib/index/kkv/test/on_disk_kkv_iterator_unittest.h"

#include "indexlib/common/chunk/chunk_decoder_creator.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, OnDiskKKVIteratorTest);

OnDiskKKVIteratorTest::OnDiskKKVIteratorTest() {}

OnDiskKKVIteratorTest::~OnDiskKKVIteratorTest() {}

void OnDiskKKVIteratorTest::CaseSetUp() {}

void OnDiskKKVIteratorTest::CaseTearDown() {}

void OnDiskKKVIteratorTest::TestSimpleProcess()
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;"
                              "cmd=add,pkey=2,skey=1,value=2,ts=1000000;"
                              "cmd=add,pkey=1,skey=2,value=3,ts=2000000;#"
                              "cmd=add,pkey=3,skey=2,value=4,ts=2000000;"
                              "cmd=delete,pkey=1,skey=2,ts=3000000;"
                              "cmd=delete,pkey=2,ts=4000000;#"
                              "cmd=add,pkey=2,skey=5,value=5,ts=5000000;";

    string expectData = "1:0:1:1;"
                        "1:2:del_skey:3;"
                        "2:0:del_pkey:4;"
                        "2:5:5:5;"
                        "3:2:4:2";
    InnerTest(docStrInSegments, expectData, PKT_DENSE, 3);
    InnerTest(docStrInSegments, expectData, PKT_CUCKOO, 3);
    InnerTest(docStrInSegments, expectData, PKT_SEPARATE_CHAIN, 3);
}

void OnDiskKKVIteratorTest::TestSamePKeyInMultiSegments()
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;#"
                              "cmd=add,pkey=1,skey=2,value=3,ts=2000000;#"
                              "cmd=delete,pkey=1,ts=3000000;#"
                              "cmd=add,pkey=1,skey=2,value=4,ts=4000000;#"
                              "cmd=add,pkey=1,skey=3,value=5,ts=5000000;#"
                              "cmd=add,pkey=1,skey=1,value=6,ts=6000000;";

    string expectData = "1:0:del_pkey:3;"
                        "1:1:6:6;"
                        "1:2:4:4;"
                        "1:3:5:5";
    InnerTest(docStrInSegments, expectData, PKT_DENSE, 1);
    InnerTest(docStrInSegments, expectData, PKT_CUCKOO, 1);
    InnerTest(docStrInSegments, expectData, PKT_SEPARATE_CHAIN, 1);
}

void OnDiskKKVIteratorTest::TestDuplicateSeyInMultiSegments()
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;#"
                              "cmd=add,pkey=1,skey=2,value=3,ts=2000000;#"
                              "cmd=delete,pkey=1,skey=2,ts=3000000;#"
                              "cmd=add,pkey=1,skey=2,value=4,ts=4000000;#"
                              "cmd=add,pkey=1,skey=0,value=5,ts=5000000;";

    string expectData = "1:0:5:5;"
                        "1:2:4:4";
    InnerTest(docStrInSegments, expectData, PKT_DENSE, 1);
    InnerTest(docStrInSegments, expectData, PKT_CUCKOO, 1);
    InnerTest(docStrInSegments, expectData, PKT_SEPARATE_CHAIN, 1);
}

void OnDiskKKVIteratorTest::TestDuplicateSeyInMultiSegments_13227243()
{
    string docStrInSegments = "cmd=add,pkey=1,skey=0,value=1,ts=1000000;cmd=add,pkey=1,skey=1,value=2,ts=2000000;#"
                              "cmd=add,pkey=1,skey=-1,value=3,ts=3000000;cmd=add,pkey=1,skey=1,value=3,ts=3000000;"
                              "cmd=delete,pkey=1,skey=0,ts=3000000;#"
                              "cmd=add,pkey=1,skey=2,value=4,ts=4000000;";

    string expectData = "1:-1:3:3;"
                        "1:0:del_skey:3;"
                        "1:1:3:3;"
                        "1:2:4:4";
    InnerTest(docStrInSegments, expectData, PKT_DENSE, 1);
    InnerTest(docStrInSegments, expectData, PKT_CUCKOO, 1);
    InnerTest(docStrInSegments, expectData, PKT_SEPARATE_CHAIN, 1);
}

void OnDiskKKVIteratorTest::InnerTest(const string& docStrs, const string& dataInfoStr, PKeyTableType tableType,
                                      size_t expectPkeyCount)
{
    tearDown();
    setUp();
    ChunkDecoderCreator::Reset();
    vector<string> docStrVec;
    StringUtil::fromString(docStrs, docStrVec, "#");
    OnDiskKKVIteratorPtr iter = CreateIterator(docStrVec, tableType);
    CheckIterator(iter, dataInfoStr, expectPkeyCount);
}

OnDiskKKVIteratorPtr OnDiskKKVIteratorTest::CreateIterator(const vector<string>& docStrVec, PKeyTableType tableType)
{
    string field = "pkey:uint32;skey:int32;value:uint32;";
    mSchema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value");
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKKVConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    KKVIndexPreference::HashDictParam param(PrefixKeyTableType2Str(tableType));
    mKKVConfig->GetIndexPreference().SetHashDictParam(param);
    mKKVConfig->GetValueConfig()->EnableCompactFormat(true);

    mOptions.GetBuildConfig(false).buildTotalMemory = 20; // 20MB
    mOptions.GetBuildConfig(true).buildTotalMemory = 20;  // 20MB

    PartitionStateMachine psm;
    psm.Init(mSchema, mOptions, GET_TEMP_DATA_PATH());
    for (size_t i = 0; i < docStrVec.size(); i++) {
        if (i == 0) {
            psm.Transfer(PE_BUILD_FULL, docStrVec[i], "", "");
        } else {
            psm.Transfer(PE_BUILD_INC, docStrVec[i], "", "");
        }
    }

    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    PartitionDataPtr partData = OnDiskPartitionData::CreateOnDiskPartitionData(GET_FILE_SYSTEM());
    vector<SegmentData> segDataVec(partData->Begin(), partData->End());
    OnDiskKKVIteratorPtr iter(new OnDiskKKVIterator(mSchema, mKKVConfig));
    iter->Init(segDataVec);
    return iter;
}

// dataInfoStr : pkey:skey:[del_pkey|del_skey|value]:ts;
void OnDiskKKVIteratorTest::CheckIterator(const OnDiskKKVIteratorPtr& iter, const string& dataInfoStr,
                                          size_t expectPkeyCount)
{
    ASSERT_EQ(expectPkeyCount, iter->EstimatePkeyCount());
    vector<string> dataInfo;
    StringUtil::fromString(dataInfoStr, dataInfo, ";");

    size_t count = 0;
    while (iter->IsValid()) {
        OnDiskSinglePKeyIterator* singlePkeyIter = iter->GetCurrentIterator();
        ASSERT_TRUE(singlePkeyIter);

        uint64_t pkey = singlePkeyIter->GetPKeyHash();
        if (singlePkeyIter->HasPKeyDeleted()) {
            string str = StringUtil::toString<uint64_t>(pkey) + ":" + StringUtil::toString<uint64_t>(0) + ":" +
                         "del_pkey:" + StringUtil::toString<uint64_t>(singlePkeyIter->GetPKeyDeletedTs());
            ASSERT_EQ(dataInfo[count++], str);
        }

        while (singlePkeyIter->IsValid()) {
            uint64_t skey = 0;
            bool isDelete = false;
            singlePkeyIter->GetCurrentSkey(skey, isDelete);

            string typeValue = "del_skey";
            if (!isDelete) {
                autil::StringView value;
                singlePkeyIter->GetCurrentValue(value);
                typeValue = StringUtil::toString<uint32_t>(*(uint32_t*)value.data());
            }
            string str = StringUtil::toString<uint64_t>(pkey) + ":" + StringUtil::toString<int64_t>((int64_t)skey) +
                         ":" + typeValue + ":" + StringUtil::toString<uint64_t>(singlePkeyIter->GetCurrentTs());
            ASSERT_EQ(dataInfo[count++], str);
            singlePkeyIter->MoveToNext();
        }
        iter->MoveToNext();
    }
    ASSERT_EQ(dataInfo.size(), count);
}
}} // namespace indexlib::index
