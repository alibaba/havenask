#include "indexlib/index/kkv/test/collect_info_processor_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/kkv/sort_collect_info_processor.h"
#include "indexlib/index/kkv/truncate_collect_info_processor.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::test;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CollectInfoProcessorTest);

CollectInfoProcessorTest::CollectInfoProcessorTest() {}

CollectInfoProcessorTest::~CollectInfoProcessorTest() {}

void CollectInfoProcessorTest::CaseSetUp()
{
    string field = "pkey:string;skey:int32;value:uint32;";
    mSchema = SchemaMaker::MakeKKVSchema(field, "pkey", "skey", "value;skey;");
    ASSERT_TRUE(mSchema.operator bool());
    const IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
    mKKVIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());

    KKVIndexFieldInfo suffixFieldInfo;
    suffixFieldInfo.fieldName = "skey";
    suffixFieldInfo.keyType = KKVKeyType::SUFFIX;
    suffixFieldInfo.countLimits = 2; // need truncate
    suffixFieldInfo.sortParams = StringToSortParams("+value;-$TIME_STAMP");
    mKKVIndexConfig->SetSuffixFieldInfo(suffixFieldInfo);
}

void CollectInfoProcessorTest::CaseTearDown() {}

void CollectInfoProcessorTest::TestNoSortNorTruncate()
{
    CollectInfos unSortInfos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,7;"
                     "4,false,9,7;"
                     "-3,true,1,2;"
                     "0,true,5,5;"
                     "2,false,2,1;";
    MakeCollectInfos(infoStr, unSortInfos);

    CollectInfoProcessor processor;
    processor.Init(mKKVIndexConfig);
    CollectInfos infos(unSortInfos.begin(), unSortInfos.end());
    processor.Process(infos, 4);
    CheckCollectInfos(infoStr, infos);
}

void CollectInfoProcessorTest::TestSortByKey()
{
    CollectInfos infos;
    string infoStr = "5,false,0,0;"
                     "1,true,0,0;"
                     "6,false,0,0;"
                     "3,true,0,0;"
                     "0,true,0,0;";
    MakeCollectInfos(infoStr, infos);

    CollectInfoKeyComp<int32_t> comp;
    sort(infos.begin(), infos.end(), comp);
    string expectStr = "0,true,0,0;"
                       "1,true,0,0;"
                       "3,true,0,0;"
                       "5,false,0,0;"
                       "6,false,0,0;";
    CheckCollectInfos(expectStr, infos);
}

void CollectInfoProcessorTest::TestSortByValue()
{
    // use value
    PackAttributeFormatter formatter;
    formatter.Init(mKKVIndexConfig->GetValueConfig()->CreatePackAttributeConfig());
    AttributeReference* ref = formatter.GetAttributeReference("value");
    CollectInfoValueComp valueComp;
    valueComp.AddSingleValueComparator(ref, true);
    valueComp.mSkeyComparator = valueComp.CreateSKeyComparator(mKKVIndexConfig, false);

    CollectInfos infos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "2,false,2,1;";
    MakeCollectInfos(infoStr, infos);

    sort(infos.begin(), infos.end(), valueComp);
    string expectStr = "0,true,5,5;"
                       "1,true,4,6;"
                       "3,true,1,2;"
                       "5,false,3,9;"
                       "6,false,7,7;"
                       "2,false,2,1;";
    CheckCollectInfos(expectStr, infos);
}

void CollectInfoProcessorTest::TestSortByTs()
{
    CollectInfoValueComp tsComp;
    tsComp.AddTimestampComparator(false);
    tsComp.mSkeyComparator = tsComp.CreateSKeyComparator(mKKVIndexConfig, false);

    CollectInfos infos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "2,false,2,1;";
    MakeCollectInfos(infoStr, infos);

    sort(infos.begin(), infos.end(), tsComp);
    string expectStr = "0,true,5,5;"
                       "1,true,4,6;"
                       "3,true,1,2;"
                       "2,false,2,1;"
                       "5,false,3,9;"
                       "6,false,7,7;";
    CheckCollectInfos(expectStr, infos);
}

void CollectInfoProcessorTest::TestMultiLayerResortBySortValue()
{
    CollectInfos unSortInfos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,7;"
                     "4,false,9,7;"
                     "-3,true,1,2;"
                     "0,true,5,5;"
                     "2,false,2,1;";
    MakeCollectInfos(infoStr, unSortInfos);

    SortCollectInfoProcessor processor;
    processor.Init(mKKVIndexConfig);
    {
        CollectInfos infos(unSortInfos.begin(), unSortInfos.end());
        infos[0]->isDeletedPKey = false;
        processor.Process(infos, 4);
        string expectStr = "-3,true,1,2;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "2,false,2,1;"
                           "4,false,9,7;";
        CheckCollectInfos(expectStr, infos);
    }
    {
        // normal case : topNum = 1, first node deletePkey is true
        CollectInfos infos(unSortInfos.begin(), unSortInfos.end());
        infos[0]->isDeletedPKey = true;
        processor.Process(infos, 3);
        string expectStr = "5,false,3,9;"
                           "-3,true,1,2;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "2,false,2,1;"
                           "4,false,9,7;";
        CheckCollectInfos(expectStr, infos);
    }
}

void CollectInfoProcessorTest::TestMultiLayerResortBySkey()
{
    CollectInfos unSortInfos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,10;"
                     "4,false,9,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "-2,false,2,8;";
    MakeCollectInfos(infoStr, unSortInfos);

    TruncateCollectInfoProcessor processor;
    processor.Init(mKKVIndexConfig);

    {
        // normal case : topNum = 1
        CollectInfos infos(unSortInfos.begin(), unSortInfos.end());
        infos[0]->isDeletedPKey = false;
        processor.Process(infos, 4);
        string expectStr = "-2,false,2,8;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;";
        CheckCollectInfos(expectStr, infos);
    }

    {
        // normal case : topNum = 1, first node deletePkey is true
        CollectInfos infos(unSortInfos.begin(), unSortInfos.end());
        infos[0]->isDeletedPKey = true;
        processor.Process(infos, 3);
        string expectStr = "5,false,3,9;"
                           "-2,false,2,8;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;";
        CheckCollectInfos(expectStr, infos);
    }

    processor.SetSKeyFieldType(ft_uint64);

    {
        // normal case : topNum = 1
        CollectInfos infos(unSortInfos.begin(), unSortInfos.end());
        infos[0]->isDeletedPKey = false;
        processor.Process(infos, 4);
        string expectStr = "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;"
                           "-2,false,2,8;";
        CheckCollectInfos(expectStr, infos);
    }

    {
        // normal case : topNum = 1, first node deletePkey is true
        CollectInfos infos(unSortInfos.begin(), unSortInfos.end());
        infos[0]->isDeletedPKey = true;
        processor.Process(infos, 3);
        string expectStr = "5,false,3,9;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;"
                           "-2,false,2,8;";
        CheckCollectInfos(expectStr, infos);
    }
}

// skey,isDeletedSkey,ts,value
void CollectInfoProcessorTest::MakeCollectInfos(const string& infoStr, CollectInfos& infos)
{
    vector<vector<string>> infoVec;
    StringUtil::fromString(infoStr, infoVec, ",", ";");

    for (size_t i = 0; i < infoVec.size(); ++i) {
        assert(infoVec[i].size() == 4);

        CollectInfo* info = mCollectInfoPool.CreateCollectInfo();
        assert(info);

        info->skey = (uint64_t)(StringUtil::fromString<int32_t>(infoVec[i][0]));
        info->isDeletedPKey = false;
        info->isDeletedSKey = (infoVec[i][1] == "true");
        info->ts = StringUtil::fromString<uint32_t>(infoVec[i][2]);

        uint32_t value = StringUtil::fromString<uint32_t>(infoVec[i][3]);
        info->value = autil::MakeCString((const char*)&value, sizeof(value), &mPool);
        infos.push_back(info);
    }
}

void CollectInfoProcessorTest::CheckCollectInfos(const string& infoStr, const CollectInfos& infos)
{
    vector<vector<string>> infoVec;
    StringUtil::fromString(infoStr, infoVec, ",", ";");

    for (size_t i = 0; i < infoVec.size(); ++i) {
        assert(infoVec[i].size() == 4);
        CollectInfo* info = infos[i];
        assert(info);

        ASSERT_EQ((uint64_t)StringUtil::fromString<int32_t>(infoVec[i][0]), info->skey);
        ASSERT_EQ(infoVec[i][1], info->isDeletedSKey ? string("true") : string("false"));
        ASSERT_EQ(StringUtil::fromString<uint32_t>(infoVec[i][2]), info->ts);

        uint32_t value = *(uint32_t*)info->value.data();
        ASSERT_EQ(StringUtil::fromString<uint32_t>(infoVec[i][3]), value);
    }
}
}} // namespace indexlib::index
