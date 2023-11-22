#pragma once

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/BlockFpEncoder.h"
#include "indexlib/util/Fp16Encoder.h"

namespace indexlib { namespace common {

class PackAttributeFormatterTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
private:
    typedef PackAttributeFormatter::PackAttributeFields PackAttributeFields;

public:
    PackAttributeFormatterTest();
    ~PackAttributeFormatterTest();

    DECLARE_CLASS_NAME(PackAttributeFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMergeAndFormatUpdateFields();
    void TestEncodeAndDecodePatchValues();
    void TestTotalErrorCountWithDefaultValue();
    void TestCompactPackAttributeFormat();
    void TestFloatEncode();
    void TestEmptyMultiString();
    void TestEnableImpactFormat();
    void TestCreatePlainFormatEncoder();

private:
    std::vector<autil::StringView> GetFields(PackAttributeFormatter* format, const document::NormalDocumentPtr& doc);

    // expectValueStr: value1,value2,value3,.....
    void CheckReference(const PackAttributeFormatter& formatter, const config::PackAttributeConfigPtr& packConfig,
                        const autil::StringView& packedField, const std::string& expectValueStr, float diff = 0.00001);

    void CheckAttrReference(const config::AttributeConfigPtr& attrConfig, const PackAttributeFormatter& formatter,
                            const autil::StringView& packFieldValue, const std::string& expectValueStr,
                            float diff = 0.00001);

    void CheckSingleValueReference(FieldType fieldType, AttributeReference* attrRef, const char* baseValue,
                                   const std::string& expectValue, float diff = 0.00001);

    void CheckMultiValueReference(FieldType fieldType, AttributeReference* attrRef, const char* baseValue,
                                  const std::string& expectValue);

    void CheckCountedMultiValueReference(FieldType fieldType, AttributeReference* attrRef, const char* baseValue,
                                         const std::string& expectValue);

    template <typename T>
    void CheckSingleValue(AttributeReference* attrRef, const char* baseAddr, const std::string& expectValueStr,
                          float diff = 0.00001);

    template <typename T>
    void CheckMultiValue(AttributeReference* attrRef, const char* baseAddr, const std::string& expectValueStr);

    template <typename T>
    void CheckCountedMultiValue(AttributeReference* attrRef, const char* baseAddr, const std::string& expectValueStr);

    void InnerTestMergeAndFormatUpdateFields(bool packAttrId, const std::string& updateFields,
                                             bool hasHashKeyInUpdateFields, const std::string& expectValueStr);

    void ConstructUpdateFields(const std::string& despStr, bool hasHashKeyInAttrField,
                               PackAttributeFields& updateFields);

private:
    void UpdateImpactValue(const config::IndexPartitionSchemaPtr& schema);

private:
    config::IndexPartitionSchemaPtr mSchema;
    document::NormalDocumentPtr mDoc;
    autil::mem_pool::Pool* mPool;
    std::string mDocStr;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(PackAttributeFormatterTestMode, PackAttributeFormatterTest,
                                     testing::Values(false, true));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestMergeAndFormatUpdateFields);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestEncodeAndDecodePatchValues);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestTotalErrorCountWithDefaultValue);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestCompactPackAttributeFormat);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestFloatEncode);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestEmptyMultiString);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestEnableImpactFormat);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PackAttributeFormatterTestMode, TestCreatePlainFormatEncoder);

//////////////////////////////////////////////////////////////////////
template <typename T>
void PackAttributeFormatterTest::CheckSingleValue(AttributeReference* attrRef, const char* baseAddr,
                                                  const std::string& expectValueStr, float diff)
{
    AttributeReferenceTyped<T>* typedRef = dynamic_cast<AttributeReferenceTyped<T>*>(attrRef);
    ASSERT_TRUE(typedRef);

    T value;
    typedRef->GetValue(baseAddr, value);

    T expectValue = autil::StringUtil::numberFromString<T>(expectValueStr);
    ASSERT_EQ(expectValue, value);
}

template <>
void PackAttributeFormatterTest::CheckSingleValue<float>(AttributeReference* attrRef, const char* baseAddr,
                                                         const std::string& expectValueStr, float diff)
{
    AttributeReferenceTyped<float>* typedRef = dynamic_cast<AttributeReferenceTyped<float>*>(attrRef);
    ASSERT_TRUE(typedRef);

    float value;
    typedRef->GetValue(baseAddr, value);

    float expectValue = autil::StringUtil::numberFromString<float>(expectValueStr);
    ASSERT_TRUE(abs(expectValue - value) <= diff);
}

template <>
void PackAttributeFormatterTest::CheckSingleValue<autil::MultiChar>(AttributeReference* attrRef, const char* baseAddr,
                                                                    const std::string& expectValueStr, float diff)
{
    AttributeReferenceTyped<autil::MultiChar>* typedRef =
        dynamic_cast<AttributeReferenceTyped<autil::MultiChar>*>(attrRef);
    ASSERT_TRUE(typedRef);

    autil::MultiChar value;
    typedRef->GetValue(baseAddr, value);

    ASSERT_EQ(expectValueStr, std::string(value.data(), value.size()));
}

template <typename T>
void PackAttributeFormatterTest::CheckMultiValue(AttributeReference* attrRef, const char* baseAddr,
                                                 const std::string& expectValueStr)
{
    AttributeReferenceTyped<autil::MultiValueType<T>>* typedRef =
        dynamic_cast<AttributeReferenceTyped<autil::MultiValueType<T>>*>(attrRef);
    ASSERT_TRUE(typedRef);

    autil::MultiValueType<T> value;
    typedRef->GetValue(baseAddr, value);

    std::vector<T> expectValues;
    autil::StringUtil::fromString(expectValueStr, expectValues, " ");

    ASSERT_EQ((uint32_t)expectValues.size(), value.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        ASSERT_EQ(expectValues[i], value[i]);
    }
}

template <>
void PackAttributeFormatterTest::CheckMultiValue<autil::MultiChar>(AttributeReference* attrRef, const char* baseAddr,
                                                                   const std::string& expectValueStr)
{
    AttributeReferenceTyped<autil::MultiString>* typedRef =
        dynamic_cast<AttributeReferenceTyped<autil::MultiString>*>(attrRef);
    ASSERT_TRUE(typedRef);

    autil::MultiString value;
    typedRef->GetValue(baseAddr, value);

    std::vector<std::string> expectValues;
    autil::StringUtil::fromString(expectValueStr, expectValues, " ");

    ASSERT_EQ((uint32_t)expectValues.size(), value.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        ASSERT_EQ(expectValues[i], std::string(value[i].data(), value[i].size()));
    }
}

template <typename T>
void PackAttributeFormatterTest::CheckCountedMultiValue(AttributeReference* attrRef, const char* baseAddr,
                                                        const std::string& expectValueStr)
{
    AttributeReferenceTyped<autil::MultiValueType<T>>* typedRef =
        dynamic_cast<AttributeReferenceTyped<autil::MultiValueType<T>>*>(attrRef);
    ASSERT_TRUE(typedRef);

    std::vector<T> expectValues;
    autil::StringUtil::fromString(expectValueStr, expectValues, " ");

    autil::MultiValueType<T> value;
    typedRef->GetValue(baseAddr, value, mPool);
    ASSERT_EQ((uint32_t)expectValues.size(), value.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        ASSERT_EQ(expectValues[i], value[i]);
    }

    // check getValue by MultiValueType
    autil::MultiValueType<T> iValue;
    typedRef->GetValue(baseAddr, iValue, mPool);
    ASSERT_EQ((uint32_t)expectValues.size(), iValue.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        ASSERT_EQ(expectValues[i], iValue[i]);
    }
}

template <>
void PackAttributeFormatterTest::CheckCountedMultiValue<float>(AttributeReference* attrRef, const char* baseAddr,
                                                               const std::string& expectValueStr)
{
    AttributeReferenceTyped<autil::MultiValueType<float>>* typedRef =
        dynamic_cast<AttributeReferenceTyped<autil::MultiValueType<float>>*>(attrRef);
    ASSERT_TRUE(typedRef);

    config::CompressTypeOption compressType = attrRef->GetCompressType();
    std::vector<float> expectValues;
    autil::StringUtil::fromString(expectValueStr, expectValues, " ");

    // check counted multi value
    autil::MultiValueType<float> value;
    typedRef->GetValue(baseAddr, value, mPool);
    ASSERT_EQ((uint32_t)expectValues.size(), value.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        if (compressType.HasBlockFpEncodeCompress()) {
            ASSERT_TRUE(abs(expectValues[i] - value[i]) <= 0.00001);
        } else if (compressType.HasFp16EncodeCompress()) {
            ASSERT_TRUE(abs(expectValues[i] - value[i]) <= 0.01);
        } else {
            ASSERT_EQ(expectValues[i], value[i]);
        }
    }

    // check multi value typed
    autil::MultiValueType<float> iValue;
    typedRef->GetValue(baseAddr, iValue, mPool);
    ASSERT_EQ((uint32_t)expectValues.size(), iValue.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        if (compressType.HasBlockFpEncodeCompress()) {
            ASSERT_TRUE(abs(expectValues[i] - iValue[i]) <= 0.00001);
        } else if (compressType.HasFp16EncodeCompress()) {
            ASSERT_TRUE(abs(expectValues[i] - iValue[i]) <= 0.01);
        } else {
            ASSERT_EQ(expectValues[i], iValue[i]);
        }
    }

    autil::mem_pool::Pool pool;
    // check str value
    std::string strValue;
    typedRef->GetStrValue(baseAddr, strValue, &pool);
    std::vector<float> actualValues;
    autil::StringUtil::fromString(strValue, actualValues, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    ASSERT_EQ(expectValues.size(), actualValues.size());
    for (size_t i = 0; i < expectValues.size(); i++) {
        if (compressType.HasBlockFpEncodeCompress()) {
            ASSERT_TRUE(abs(expectValues[i] - actualValues[i]) <= 0.00001);
        } else if (compressType.HasFp16EncodeCompress()) {
            ASSERT_TRUE(abs(expectValues[i] - actualValues[i]) <= 0.01);
        } else {
            ASSERT_EQ(expectValues[i], actualValues[i]) << expectValueStr << "," << strValue;
        }
    }
    // check data value
    autil::StringView expectedDataValue;
    if (compressType.HasBlockFpEncodeCompress()) {
        expectedDataValue = util::BlockFpEncoder::Encode(expectValues.data(), expectValues.size(), mPool);
    } else if (compressType.HasFp16EncodeCompress()) {
        expectedDataValue = util::Fp16Encoder::Encode(expectValues.data(), expectValues.size(), mPool);
    } else {
        expectedDataValue = autil::StringView((char*)expectValues.data(), expectValues.size() * sizeof(float));
    }
    ASSERT_EQ(expectedDataValue, typedRef->GetDataValue(baseAddr).valueStr);
}
}} // namespace indexlib::common
