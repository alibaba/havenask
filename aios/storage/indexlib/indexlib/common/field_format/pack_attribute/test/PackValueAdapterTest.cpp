#include "indexlib/common/field_format/pack_attribute/PackValueAdapter.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::document;
using namespace indexlib::test;

namespace indexlib { namespace common {

class PackValueAdapterTest : public TESTBASE
{
public:
    PackValueAdapterTest();
    ~PackValueAdapterTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    void PreparePackAttributeConfig(const string& attrNames, bool enablePlainFormat);

    /* defaultValues:f1=v1,f2=v2 */
    void SetDefaultValue(const std::string& defaultValues);

    /* fieldValues:f1=v1,f2=v2,... */
    autil::StringView CreatePackValue(const std::string& fieldValues);

    /* valueStr: f1=v1,f2=v2, ... */
    void CheckValues(autil::StringView packValue, const std::string& valueStr);

    void InnerTest(const std::string& oldPackConfig, bool oldPlainFormat, const std::string& oldFieldValues,
                   const std::string& newPackConfig, bool newPlainFormat, const std::string& newFieldValues);

private:
    autil::mem_pool::Pool _pool;
    PackAttributeConfigPtr _packAttrConf;
    IndexPartitionSchemaPtr _schema;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.common, PackValueAdapterTest);

PackValueAdapterTest::PackValueAdapterTest() {}

PackValueAdapterTest::~PackValueAdapterTest() {}

void PackValueAdapterTest::PreparePackAttributeConfig(const string& attrNames, bool enablePlainFormat)
{
    string field = "int8_single:int8;int16_single:int16;int32_single:int32;int64_single:int64;"
                   "int8_multi:int8:true;int16_multi:int16:true;int32_multi:int32:true;int64_multi:int64:true;"
                   "float_single:float;float_single_fp16:float:false:false:fp16;"
                   "float_multi:float:true:false:block_fp:2;str_single:string;str_multi:string:true";

    string index = "pk:primarykey64:str_single";

    vector<string> attrNameVec;
    vector<string> defaultValueVec;
    vector<string> attrInfos;
    autil::StringUtil::fromString(attrNames, attrInfos, ",");

    for (auto& attrInfo : attrInfos) {
        vector<string> tmpVec;
        autil::StringUtil::fromString(attrInfo, tmpVec, "=");
        if (tmpVec.size() == 2) {
            defaultValueVec.push_back(attrInfo);
            attrNameVec.push_back(tmpVec[0]);
        } else {
            attrNameVec.push_back(attrInfo);
        }
    }

    string attribute = "pack_attr:" + autil::StringUtil::toString(attrNameVec, ",");
    _schema = SchemaMaker::MakeSchema(field, index, attribute, "");
    _packAttrConf = _schema->GetAttributeSchema()->GetPackAttributeConfig("pack_attr");
    if (enablePlainFormat) {
        _packAttrConf->EnablePlainFormat();
    }
    SetDefaultValue(autil::StringUtil::toString(defaultValueVec, ","));
}

void PackValueAdapterTest::SetDefaultValue(const string& defaultValues)
{
    vector<vector<string>> kvMap;
    autil::StringUtil::fromString(defaultValues, kvMap, "=", ",");
    for (auto& kv : kvMap) {
        assert(kv.size() == 2);
        const std::vector<AttributeConfigPtr>& configVec = _packAttrConf->GetAttributeConfigVec();
        for (auto& config : configVec) {
            if (config->GetAttrName() == kv[0]) {
                config->GetFieldConfig()->SetDefaultValue(kv[1]);
                break;
            }
        }
    }
}

autil::StringView PackValueAdapterTest::CreatePackValue(const std::string& fieldValues)
{
    string docStr = "cmd=add," + fieldValues;
    NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(_schema, docStr);

    PackAttributeFormatter packFormatter;
    packFormatter.Init(_packAttrConf);

    vector<autil::StringView> fields;
    const PackAttributeFormatter::FieldIdVec& fieldIds = packFormatter.GetFieldIds();
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();
    for (auto id : fieldIds) {
        fields.push_back(attrDoc->GetField(id));
    }

    autil::StringView packedField = packFormatter.Format(fields, &_pool);
    AttributeConfigPtr packDataAttrConfig = _packAttrConf->CreateAttributeConfig();
    AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));
    autil::StringView encodePackData = convertor->Decode(packedField).data;

    std::shared_ptr<PlainFormatEncoder> plainFormatEncoder(packFormatter.CreatePlainFormatEncoder());
    if (plainFormatEncoder) {
        char* buffer = (char*)_pool.allocate(encodePackData.size());
        autil::StringView plainValue;
        auto ret = plainFormatEncoder->Encode(encodePackData, buffer, encodePackData.size(), plainValue);
        (void)ret;
        encodePackData = plainValue;
    }

    size_t countLen = 0;
    autil::MultiValueFormatter::decodeCount(encodePackData.data(), countLen);
    const char* data = encodePackData.data() + countLen;
    return autil::StringView(data, encodePackData.size() - countLen);
}

void PackValueAdapterTest::CheckValues(autil::StringView packValue, const std::string& valueStr)
{
    PackAttributeFormatter packFormatter;
    packFormatter.Init(_packAttrConf);

    std::shared_ptr<PlainFormatEncoder> plainFormatEncoder(packFormatter.CreatePlainFormatEncoder());
    if (plainFormatEncoder) {
        autil::StringView tmpValue;
        auto ret = plainFormatEncoder->Decode(packValue, &_pool, tmpValue);
        (void)ret;
        packValue = tmpValue;
    }

    vector<vector<string>> expectValues;
    autil::StringUtil::fromString(valueStr, expectValues, "=", ",");
    for (auto& kv : expectValues) {
        assert(kv.size() == 1 || kv.size() == 2);
        AttributeReference* ref = packFormatter.GetAttributeReference(kv[0]);
        ASSERT_TRUE(ref);

        string value;
        ref->GetStrValue(packValue.data(), value, &_pool);

        if (kv.size() == 2) {
            autil::StringUtil::replaceAll(value, "", " ");
            ASSERT_EQ(kv[1], value);
        }
        if (kv.size() == 1) {
            ASSERT_TRUE(value.empty());
        }
    }
}

TEST_F(PackValueAdapterTest, TestSimpleProcess)
{
    InnerTest(
        /*old_pack_define*/ "int32_single,float_single,int32_multi", /*old_plain_format*/ true,
        /*old_fields*/ "int32_single=10,float_single=11,int32_multi=1 3 5",
        /*pack_define*/ "float_single,float_single_fp16=12,float_multi,int64_single=100,int64_multi=1,int32_multi",
        /*plain_format*/ false,
        /*fields*/
        "float_single=11,float_single_fp16=12,float_multi=0 0,int64_single=100,int64_multi=1,int32_multi=1 3 5");

    InnerTest(
        "int32_single,float_single,float_single_fp16,float_multi,int32_multi,str_multi", false,
        "int32_single=10,float_single=11,float_single_fp16=12,float_multi=2 3,int32_multi=1 3 5,str_multi=abc efg",
        "float_single,float_single_fp16,float_multi,int64_single=100,int64_multi=1,str_multi", true,
        "float_single=11,float_single_fp16=12,float_multi=2 3,int64_single=100,int64_multi=1,str_multi=abc efg");

    InnerTest("int32_single,float_single,int32_multi", false, "int32_single=10,float_single=11,int32_multi=1 3 5",
              "float_single,float_single_fp16=13,int64_single=100,int64_multi=1,str_multi=abc", false,
              "float_single=11,float_single_fp16=13,int64_single=100,int64_multi=1,str_multi=abc");

    InnerTest("int32_single,float_single,int32_multi", true, "int32_single=10,float_single=11,int32_multi=1 3 5",
              "float_single,int64_single=100,int64_multi=1,str_multi=abc", true,
              "float_single=11,int64_single=100,int64_multi=1,str_multi=abc");

    InnerTest("int32_single,float_single,int32_multi,str_multi", false,
              "int32_single=10,float_single=11,int32_multi=1 3 5",
              "float_single,int64_single=100,int64_multi=1,str_multi", true,
              "float_single=11,int64_single=100,int64_multi=1,str_multi=");
}

TEST_F(PackValueAdapterTest, TestSameFieldsWithDiffPlainFormat)
{
    InnerTest(/*old_pack_define*/ "int32_single,float_single,float_single_fp16,int32_multi", /*old_plain_format*/ true,
              /*old_fields*/ "int32_single=10,float_single=11,float_single_fp16=12,int32_multi=1 3 5",
              /*pack_define*/ "int32_single,float_single,float_single_fp16,int32_multi", /*plain_format*/ false,
              /*fields*/ "int32_single=10,float_single=11,float_single_fp16=12,int32_multi=1 3 5");

    InnerTest(/*old_pack_define*/ "int32_single,float_single,float_single_fp16,int32_multi", /*old_plain_format*/ false,
              /*old_fields*/ "int32_single=10,float_single=11,float_single_fp16=12,int32_multi=1 3 5",
              /*pack_define*/ "int32_single,float_single,float_single_fp16,int32_multi", /*plain_format*/ true,
              /*fields*/ "int32_single=10,float_single=11,float_single_fp16=12,int32_multi=1 3 5");
}

TEST_F(PackValueAdapterTest, TestSameFieldWithDiffCompressType)
{
    PreparePackAttributeConfig("int32_single,float_single,int32_multi", true);
    auto packValue = CreatePackValue("int32_single=10,float_single=11,int32_multi=1 3 5");

    auto oldPackAttrConfig = _packAttrConf;
    PreparePackAttributeConfig("int32_single=20,float_single,int32_multi", true);
    const std::vector<AttributeConfigPtr>& configVec = _packAttrConf->GetAttributeConfigVec();
    for (auto& config : configVec) {
        if (config->GetAttrName() == "float_single") {
            ASSERT_TRUE(config->SetCompressType("fp16").IsOK());
            config->GetFieldConfig()->SetDefaultValue("14");
            break;
        }
    }
    PackValueAdapter adapter;
    ASSERT_TRUE(adapter.Init(oldPackAttrConfig, _packAttrConf));
    autil::StringView newPackValue = adapter.ConvertIndexPackValue(packValue, &_pool);
    CheckValues(newPackValue, "int32_single=10,float_single=14,int32_multi=1 3 5");
}

TEST_F(PackValueAdapterTest, TestIgnoreFields)
{
    PreparePackAttributeConfig("int32_single,float_single,int32_multi", true);
    auto packValue = CreatePackValue("int32_single=10,float_single=11,int32_multi=1 3 5");

    auto oldPackAttrConfig = _packAttrConf;
    PreparePackAttributeConfig("int32_single=20,float_single,int32_multi", true);

    PackValueAdapter adapter;
    ASSERT_TRUE(adapter.Init(oldPackAttrConfig, _packAttrConf, {"int32_single"}));
    autil::StringView newPackValue = adapter.ConvertIndexPackValue(packValue, &_pool);
    CheckValues(newPackValue, "int32_single=20,float_single=11,int32_multi=1 3 5");
}

TEST_F(PackValueAdapterTest, TestInitFailWithImpactFormat)
{
    PreparePackAttributeConfig("int32_single,float_single,int32_multi,str_multi", false);
    auto oldPackAttrConfig = _packAttrConf;

    PreparePackAttributeConfig("int32_single,float_single,int32_multi", false);
    _packAttrConf->EnableImpact();

    PackValueAdapter adapter;
    ASSERT_FALSE(adapter.Init(oldPackAttrConfig, _packAttrConf));
}

void PackValueAdapterTest::InnerTest(const std::string& oldPackConfig, bool oldPlainFormat,
                                     const std::string& oldFieldValues, const std::string& newPackConfig,
                                     bool newPlainFormat, const std::string& newFieldValues)
{
    PreparePackAttributeConfig(oldPackConfig, oldPlainFormat);
    auto packValue = CreatePackValue(oldFieldValues);

    auto oldPackAttrConfig = _packAttrConf;
    PreparePackAttributeConfig(newPackConfig, newPlainFormat);

    PackValueAdapter adapter;
    ASSERT_TRUE(adapter.Init(oldPackAttrConfig, _packAttrConf));
    autil::StringView newPackValue = adapter.ConvertIndexPackValue(packValue, &_pool);
    CheckValues(newPackValue, newFieldValues);
}

}} // namespace indexlib::common
