#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/dump/KKVDocComparator.h"
#include "indexlib/index/kkv/dump/NormalKKVDocSorter.h"
#include "indexlib/index/kkv/dump/SKeyCollectInfoPool.h"
#include "indexlib/index/kkv/dump/SKeyHashComparator.h"
#include "indexlib/index/kkv/dump/TruncateKKVDocSorter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;

namespace indexlibv2::index {

class KKVDocSorterTest : public TESTBASE
{
public:
    void setUp() override;

protected:
    using SKeyCollectInfos = std::vector<SKeyCollectInfo*>;
    // skey,isDel,ts,value;......
    void MakeSKeyCollectInfos(const std::string& infoStr, SKeyCollectInfos& infos);
    void CheckSKeyCollectInfos(const std::string& infoStr, const SKeyCollectInfos& infos);

protected:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _kkvIndexConfig;
    SKeyCollectInfoPool _collectInfoPool;
};

void KKVDocSorterTest::setUp()
{
    string schemaJsonStr = R"(
{
    "index_name": "pkey_skey",
    "index_type": "PRIMARY_KEY",
    "index_fields": [
        {
            "field_name": "pkey",
            "key_type": "prefix"
        },
        {
            "field_name": "skey",
            "key_type": "suffix",
            "count_limits": 2,
            "trunc_sort_params": "+value;-$TIME_STAMP"
        }
    ],
    "value_fields": [
         "value"
    ]
}
    )";

    auto fieldConfigs = config::FieldConfig::TEST_CreateFields("pkey:string;skey:int32;value:uint32");
    config::MutableJson runtimeSettings;
    config::IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    _kkvIndexConfig = std::make_shared<indexlibv2::config::KKVIndexConfig>();
    auto any = autil::legacy::json::ParseJson(schemaJsonStr);
    ASSERT_NO_THROW(_kkvIndexConfig->Deserialize(any, 0, resource));
}

// skey,isDeletedSkey,ts,value
void KKVDocSorterTest::MakeSKeyCollectInfos(const string& infoStr, SKeyCollectInfos& infos)
{
    vector<vector<string>> infoVec;
    StringUtil::fromString(infoStr, infoVec, ",", ";");

    for (size_t i = 0; i < infoVec.size(); ++i) {
        ASSERT_TRUE(infoVec[i].size() == 4);

        auto info = _collectInfoPool.CreateCollectInfo();
        ASSERT_TRUE(info);

        info->skey = (uint64_t)(StringUtil::fromString<int32_t>(infoVec[i][0]));
        info->isDeletedPKey = false;
        info->isDeletedSKey = (infoVec[i][1] == "true");
        info->ts = StringUtil::fromString<uint32_t>(infoVec[i][2]);

        uint32_t value = StringUtil::fromString<uint32_t>(infoVec[i][3]);
        info->value = autil::MakeCString((const char*)&value, sizeof(value), &_pool);
        infos.push_back(info);
    }
}

void KKVDocSorterTest::CheckSKeyCollectInfos(const string& infoStr, const SKeyCollectInfos& infos)
{
    vector<vector<string>> infoVec;
    StringUtil::fromString(infoStr, infoVec, ",", ";");

    for (size_t i = 0; i < infoVec.size(); ++i) {
        ASSERT_TRUE(infoVec[i].size() == 4);
        auto info = infos[i];
        ASSERT_TRUE(info);

        ASSERT_EQ((uint64_t)StringUtil::fromString<int32_t>(infoVec[i][0]), info->skey) << i;
        ASSERT_EQ(infoVec[i][1], info->isDeletedSKey ? string("true") : string("false")) << i;
        ASSERT_EQ(StringUtil::fromString<uint32_t>(infoVec[i][2]), info->ts) << i;

        uint32_t value = *(uint32_t*)info->value.data();
        ASSERT_EQ(StringUtil::fromString<uint32_t>(infoVec[i][3]), value) << i;
    }
}

TEST_F(KKVDocSorterTest, TestSortByKey)
{
    SKeyCollectInfos infos;
    string infoStr = "5,false,0,0;"
                     "1,true,0,0;"
                     "6,false,0,0;"
                     "3,true,0,0;"
                     "0,true,0,0;";
    MakeSKeyCollectInfos(infoStr, infos);

    SKeyHashComparator<int32_t> comp;
    sort(infos.begin(), infos.end(), comp);
    string expectStr = "0,true,0,0;"
                       "1,true,0,0;"
                       "3,true,0,0;"
                       "5,false,0,0;"
                       "6,false,0,0;";
    CheckSKeyCollectInfos(expectStr, infos);
}

TEST_F(KKVDocSorterTest, TestSortByValue)
{
    // use value
    PackAttributeFormatter formatter;
    auto [status, packAttrConfig] = _kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(status.IsOK());
    formatter.Init(packAttrConfig);
    auto ref = formatter.GetAttributeReference("value");
    KKVDocComparator valueComp;
    valueComp.AddSingleValueComparator(ref, true);
    valueComp._sKeyFieldComparator = valueComp.CreateSKeyFieldComparator(_kkvIndexConfig, false);

    SKeyCollectInfos infos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "2,false,2,1;";
    MakeSKeyCollectInfos(infoStr, infos);

    sort(infos.begin(), infos.end(), valueComp);
    string expectStr = "0,true,5,5;"
                       "1,true,4,6;"
                       "3,true,1,2;"
                       "5,false,3,9;"
                       "6,false,7,7;"
                       "2,false,2,1;";
    CheckSKeyCollectInfos(expectStr, infos);
}

TEST_F(KKVDocSorterTest, TestSortByTs)
{
    KKVDocComparator tsComp;
    tsComp.AddTimestampComparator(false);
    tsComp._sKeyFieldComparator = tsComp.CreateSKeyFieldComparator(_kkvIndexConfig, false);

    SKeyCollectInfos infos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "2,false,2,1;";
    MakeSKeyCollectInfos(infoStr, infos);

    sort(infos.begin(), infos.end(), tsComp);
    string expectStr = "0,true,5,5;"
                       "1,true,4,6;"
                       "3,true,1,2;"
                       "2,false,2,1;"
                       "5,false,3,9;"
                       "6,false,7,7;";
    CheckSKeyCollectInfos(expectStr, infos);
}

TEST_F(KKVDocSorterTest, TestMultiLayerResortBySortValue)
{
    SKeyCollectInfos unSortInfos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,7;"
                     "4,false,9,7;"
                     "-3,true,1,2;"
                     "0,true,5,5;"
                     "2,false,2,1;";
    MakeSKeyCollectInfos(infoStr, unSortInfos);

    NormalKKVDocSorter kkvDocSorter(_kkvIndexConfig);
    ASSERT_TRUE(kkvDocSorter.Init());
    {
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            ASSERT_FALSE(isDeletedPkey);
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "-3,true,1,2;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "2,false,2,1;"
                           "4,false,9,7;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
    kkvDocSorter.Reset();
    {
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            if (kkvDocSorter.Size() == 0) {
                // normal case : topNum = 1, first node deletePkey is true
                isDeletedPkey = true;
            }
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "5,false,3,9;"
                           "-3,true,1,2;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "2,false,2,1;"
                           "4,false,9,7;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
}

TEST_F(KKVDocSorterTest, TestMultiLayerResortBySkeyInt32)
{
    SKeyCollectInfos unSortInfos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,10;"
                     "4,false,9,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "-2,false,2,8;";
    MakeSKeyCollectInfos(infoStr, unSortInfos);

    TruncateKKVDocSorter kkvDocSorter(_kkvIndexConfig);
    ASSERT_TRUE(kkvDocSorter.Init());

    {
        // normal case : topNum = 1
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "-2,false,2,8;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
    kkvDocSorter.Reset();
    {
        // normal case : topNum = 1, first node deletePkey is true
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            if (kkvDocSorter.Size() == 0) {
                // normal case : topNum = 1, first node deletePkey is true
                isDeletedPkey = true;
            }
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "5,false,3,9;"
                           "-2,false,2,8;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
}

TEST_F(KKVDocSorterTest, TestMultiLayerResortBySkeyUint64)
{
    SKeyCollectInfos unSortInfos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,10;"
                     "4,false,9,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "-2,false,2,8;";
    MakeSKeyCollectInfos(infoStr, unSortInfos);

    TruncateKKVDocSorter kkvDocSorter(_kkvIndexConfig);
    auto suffixFieldConfig = std::make_shared<config::FieldConfig>();
    autil::legacy::FromJsonString(*suffixFieldConfig, R"({"field_name":"skey","field_type":"UINT64"})");
    _kkvIndexConfig->SetSuffixFieldConfig(suffixFieldConfig);
    ASSERT_TRUE(kkvDocSorter.Init());
    {
        // normal case : topNum = 1
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;"
                           "-2,false,2,8;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
    kkvDocSorter.Reset();
    {
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            if (kkvDocSorter.Size() == 0) {
                // normal case : topNum = 1, first node deletePkey is true
                isDeletedPkey = true;
            }
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "5,false,3,9;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;"
                           "-2,false,2,8;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
}

// 虽然SKEY类型是STRING，但是SKEY HASH还是UINT64类型，故结果与case TestMultiLayerResortBySkeyUint64一致
TEST_F(KKVDocSorterTest, TestMultiLayerResortBySkeyString)
{
    SKeyCollectInfos unSortInfos;
    string infoStr = "5,false,3,9;"
                     "1,true,4,6;"
                     "6,false,7,10;"
                     "4,false,9,7;"
                     "3,true,1,2;"
                     "0,true,5,5;"
                     "-2,false,2,8;";
    MakeSKeyCollectInfos(infoStr, unSortInfos);

    TruncateKKVDocSorter kkvDocSorter(_kkvIndexConfig);
    auto suffixFieldConfig = std::make_shared<config::FieldConfig>();
    autil::legacy::FromJsonString(*suffixFieldConfig, R"({"field_name":"skey","field_type":"STRING"})");
    _kkvIndexConfig->SetSuffixFieldConfig(suffixFieldConfig);
    ASSERT_TRUE(kkvDocSorter.Init());
    {
        // normal case : topNum = 1
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;"
                           "-2,false,2,8;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
    kkvDocSorter.Reset();
    {
        for (auto sortInfo : unSortInfos) {
            auto [isDeletedPkey, kkvdoc] = sortInfo->ToKKVDoc();
            if (kkvDocSorter.Size() == 0) {
                // normal case : topNum = 1, first node deletePkey is true
                isDeletedPkey = true;
            }
            kkvDocSorter.AddDoc(isDeletedPkey, kkvdoc);
        }
        kkvDocSorter.Sort();
        string expectStr = "5,false,3,9;"
                           "0,true,5,5;"
                           "1,true,4,6;"
                           "3,true,1,2;"
                           "4,false,9,7;"
                           "-2,false,2,8;";
        CheckSKeyCollectInfos(expectStr, kkvDocSorter.TEST_GetCollectInfos());
    }
}

} // namespace indexlibv2::index
