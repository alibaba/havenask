#include "indexlib/config/test/pack_attribute_config_unittest.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, PackAttributeConfigTest);

PackAttributeConfigTest::PackAttributeConfigTest() {}

PackAttributeConfigTest::~PackAttributeConfigTest() {}

void PackAttributeConfigTest::TestCreateAttributeConfig()
{
    // single string
    FieldConfigPtr stringField(new FieldConfig("string", ft_string, false, true, false));
    AttributeConfigPtr stringAttr(new AttributeConfig(AttributeConfig::ct_normal));
    stringAttr->Init(stringField);
    // single fixed string
    FieldConfigPtr fixedStringField(new FieldConfig("fixedString", ft_string, false, true, false));
    fixedStringField->SetFixedMultiValueCount(5);
    AttributeConfigPtr fixedStringAttr(new AttributeConfig(AttributeConfig::ct_normal));
    fixedStringAttr->Init(fixedStringField);
    // single int16
    FieldConfigPtr int16Field(new FieldConfig("int16", ft_int16, false, true, false));
    AttributeConfigPtr int16Attr(new AttributeConfig(AttributeConfig::ct_normal));
    int16Attr->Init(int16Field);
    // single int64
    FieldConfigPtr int64Field(new FieldConfig("int64", ft_int64, false, true, false));
    AttributeConfigPtr int64Attr(new AttributeConfig(AttributeConfig::ct_normal));
    int64Attr->Init(int64Field);

    // multi int32
    FieldConfigPtr multiInt32Field(new FieldConfig("multiInt32", ft_int32, true, true, false));
    AttributeConfigPtr multiInt32Attr(new AttributeConfig(AttributeConfig::ct_normal));
    multiInt32Attr->Init(multiInt32Field);
    // fixed multi int32
    FieldConfigPtr fixedMultiInt32Field(new FieldConfig("fixedMultiInt32", ft_int32, true, true, false));
    fixedMultiInt32Field->SetFixedMultiValueCount(8);
    AttributeConfigPtr fixedMultiInt32Attr(new AttributeConfig(AttributeConfig::ct_normal));
    fixedMultiInt32Attr->Init(fixedMultiInt32Field);

    CompressTypeOption compressType;
    ASSERT_TRUE(compressType.Init("uniq").IsOK());
    // 1.total length is fixed, mEnableCompactFormat=true
    // single int16, fixed multi int32
    {
        PackAttributeConfigPtr packAttrConfig(new PackAttributeConfig("attr", compressType,
                                                                      index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT,
                                                                      std::shared_ptr<FileCompressConfig>()));
        ASSERT_TRUE(packAttrConfig->AddAttributeConfig(int16Attr).IsOK());
        ASSERT_TRUE(packAttrConfig->AddAttributeConfig(fixedMultiInt32Attr).IsOK());

        AttributeConfigPtr packAttr = packAttrConfig->CreateAttributeConfig();
        ASSERT_FALSE(packAttr->IsLengthFixed());

        std::shared_ptr<FileCompressConfig> fileCompressConfig(new FileCompressConfig);
        string fileCompressConfigStr = R"({
            "name":"compress2",
            "type":"snappy",
            "level":"12"
        })";
        FromJsonString(*fileCompressConfig, fileCompressConfigStr);
        PackAttributeConfigPtr packAttrConfig2(new PackAttributeConfig(
            "attr", compressType, index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT, fileCompressConfig));
        ASSERT_TRUE(packAttrConfig2->AddAttributeConfig(int16Attr).IsOK());
        ASSERT_TRUE(packAttrConfig2->AddAttributeConfig(fixedMultiInt32Attr).IsOK());
        ASSERT_THROW(packAttrConfig->AssertEqual(*packAttrConfig2), util::BadParameterException);
        ASSERT_NO_THROW(packAttrConfig->AssertEqual(*packAttrConfig));
    }
}
}} // namespace indexlib::config
