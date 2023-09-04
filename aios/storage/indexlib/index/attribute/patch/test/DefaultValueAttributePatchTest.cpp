#include "indexlib/index/attribute/patch/DefaultValueAttributePatch.h"

#include <optional>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/common/field_format/attribute/AttributeFieldPrinter.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

template <typename T>
struct TypeParseTraits;

#define REGISTER_PARSE_TYPE(X, Y)                                                                                      \
    template <>                                                                                                        \
    struct TypeParseTraits<X> {                                                                                        \
        static const FieldType type;                                                                                   \
    };                                                                                                                 \
    const FieldType TypeParseTraits<X>::type = Y

REGISTER_PARSE_TYPE(uint8_t, ft_uint8);
REGISTER_PARSE_TYPE(int8_t, ft_int8);
REGISTER_PARSE_TYPE(uint16_t, ft_uint16);
REGISTER_PARSE_TYPE(int16_t, ft_int16);
REGISTER_PARSE_TYPE(uint32_t, ft_uint32);
REGISTER_PARSE_TYPE(int32_t, ft_int32);
REGISTER_PARSE_TYPE(uint64_t, ft_uint64);
REGISTER_PARSE_TYPE(int64_t, ft_int64);
REGISTER_PARSE_TYPE(float, ft_float);
REGISTER_PARSE_TYPE(double, ft_double);

template <typename T>
class FakeSingleAttributePatch : public IAttributePatch
{
public:
    FakeSingleAttributePatch(std::map<docid_t, T> values) : _values(std::move(values)) {}
    std::pair<Status, size_t> Seek(docid_t docId, uint8_t* value, size_t maxLen, bool& isNull) override
    {
        isNull = false;
        auto iter = _values.find(docId);
        if (iter == _values.end()) {
            return {Status::OK(), 0};
        }
        *((T*)value) = iter->second;
        return {Status::OK(), sizeof(T)};
    };
    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull) override { return false; }
    uint32_t GetMaxPatchItemLen() const override { return _values.size(); }

private:
    std::map<docid_t, T> _values;
};

class DefaultValueAttributePatchTest : public TESTBASE
{
public:
    DefaultValueAttributePatchTest() = default;
    ~DefaultValueAttributePatchTest() = default;

public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename T, FieldType fieldType = TypeParseTraits<T>::type>
    void InnerTestSingleAttribute(std::string valueStr, std::optional<T> expectedValue);

    template <typename T, FieldType fieldType = TypeParseTraits<T>::type>
    void InnerTestSingleAttributeWithPatch(std::string defaultValueStr, T defaultValue,
                                           std::map<docid_t, T> patchValue);

    template <typename T>
    void InnerCheck(DefaultValueAttributePatch& reader, docid_t docId, const std::vector<T>& expectedValues,
                    std::optional<size_t> fixedCount);

    template <typename T, FieldType fieldType = TypeParseTraits<T>::type>
    void InnerTestMultiAttribute(std::vector<std::string> defaultValueStr, std::optional<std::vector<T>> expectedValue,
                                 std::optional<size_t> fixedCount);
};

void DefaultValueAttributePatchTest::setUp() {}

void DefaultValueAttributePatchTest::tearDown() {}

template <typename T, FieldType fieldType>
void DefaultValueAttributePatchTest::InnerTestSingleAttribute(std::string valueStr, std::optional<T> expectedValue)
{
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", fieldType, false);
    fieldConfig->SetDefaultValue(valueStr);
    auto attrConfig = std::make_shared<AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
    DefaultValueAttributePatch patch;
    auto status = patch.Open(attrConfig);

    ASSERT_EQ(expectedValue.has_value(), status.IsOK());
    if (expectedValue.has_value()) {
        T value;
        bool isNull;
        auto [status, length] = patch.Seek(0, (uint8_t*)&value, sizeof(T), isNull);
        ASSERT_EQ(sizeof(T), length);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(expectedValue.value(), value);
        AttributeFieldPrinter printer;
        printer.Init(fieldConfig);

        std::string output;
        ASSERT_TRUE(printer.Print(isNull, value, &output));
        if (valueStr.empty() && (fieldType == ft_time || fieldType == ft_date || fieldType == ft_timestamp)) {
            return;
        }
        valueStr = valueStr.empty() ? "0" : valueStr;
        ASSERT_EQ(valueStr, output);
    }
}

template <typename T, FieldType fieldType>
void DefaultValueAttributePatchTest::InnerTestSingleAttributeWithPatch(std::string defaultValueStr, T defaultValue,
                                                                       std::map<docid_t, T> patchValue)
{
    assert(patchValue.find(0) == patchValue.end());
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", fieldType, false);
    fieldConfig->SetDefaultValue(defaultValueStr);
    auto attrConfig = std::make_shared<AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
    DefaultValueAttributePatch defaultPatch;
    ASSERT_TRUE(defaultPatch.Open(attrConfig).IsOK());
    defaultPatch.SetPatchReader(std::make_shared<FakeSingleAttributePatch<T>>(patchValue));

    auto InnerCheck = [&](auto docId, T expectedValue) {
        T value;
        bool isNull;
        auto [status, length] = defaultPatch.Seek(docId, (uint8_t*)&value, sizeof(T), isNull);
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(sizeof(T), length);
        ASSERT_EQ(expectedValue, value);
        ASSERT_FALSE(isNull);
    };

    InnerCheck(0, defaultValue);
    for (auto& [docId, expectedValue] : patchValue) {
        InnerCheck(docId, expectedValue);
    }
}
template <typename T>
void DefaultValueAttributePatchTest::InnerCheck(DefaultValueAttributePatch& patch, docid_t docId,
                                                const std::vector<T>& expectedValues, std::optional<size_t> fixedCount)
{
    uint8_t buffer[1024];
    bool isNull;
    auto [status, length] = patch.Seek(0, buffer, sizeof(buffer), isNull);
    ASSERT_TRUE(status.IsOK());
    ASSERT_GT(length, 0);
    autil::MultiValueType<T> value;
    if (fixedCount == std::nullopt) {
        value.init(buffer);
    } else {
        value.init(buffer, fixedCount.value());
    }
    std::vector<T> values;
    for (size_t i = 0; i < value.size(); ++i) {
        values.push_back(value[i]);
    }
    ASSERT_EQ(expectedValues, values);
}

template <typename T, FieldType fieldType>
void DefaultValueAttributePatchTest::InnerTestMultiAttribute(std::vector<std::string> defaultValueStr,
                                                             std::optional<std::vector<T>> expectedValue,
                                                             std::optional<size_t> fixedCount)
{
    std::string str;
    if (!defaultValueStr.empty()) {
        for (auto valueStr : defaultValueStr) {
            str += valueStr + INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
        }
        str.pop_back();
    }

    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", fieldType, true);
    fieldConfig->SetDefaultValue(str);
    if (fixedCount != std::nullopt) {
        fieldConfig->SetFixedMultiValueCount(fixedCount.value());
        assert(expectedValue == std::nullopt || expectedValue.value().size() == fixedCount.value());
    }
    DefaultValueAttributePatch patch;
    auto attrConfig = std::make_shared<AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
    auto status = patch.Open(attrConfig);

    ASSERT_EQ(expectedValue.has_value(), status.IsOK());
    if (expectedValue.has_value()) {
        InnerCheck(patch, 0, expectedValue.value(), fixedCount);
    }
}

TEST_F(DefaultValueAttributePatchTest, testUsage)
{
    auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", ft_int64, false);
    fieldConfig->SetDefaultValue("114514");
    auto attrConfig = std::make_shared<AttributeConfig>();
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
    DefaultValueAttributePatch patch;
    ASSERT_TRUE(patch.Open(attrConfig).IsOK());
    ASSERT_EQ(8, patch.GetMaxPatchItemLen());

    int64_t value = 0;
    bool isNull = true;
    auto [status, length] = patch.Seek(0, (uint8_t*)&value, 8, isNull);
    ASSERT_TRUE(status.IsOK());
    ASSERT_EQ(8, length);
    ASSERT_EQ(114514, value);
    ASSERT_FALSE(isNull);
}

TEST_F(DefaultValueAttributePatchTest, testSingleAttribute)
{
    InnerTestSingleAttribute<int8_t>("-12", -12);
    InnerTestSingleAttribute<uint8_t>("1", 1);
    InnerTestSingleAttribute<int16_t>("345", 345);
    InnerTestSingleAttribute<uint16_t>("213", 213);
    InnerTestSingleAttribute<int32_t>("30000", 30000);
    InnerTestSingleAttribute<uint32_t>("65535", 65535);
    InnerTestSingleAttribute<int64_t>("-12300000", -12300000);
    InnerTestSingleAttribute<uint64_t>("123456789", 123456789);
    InnerTestSingleAttribute<float>("12.3", 12.3);
    InnerTestSingleAttribute<double>("123.4", 123.4);

    InnerTestSingleAttribute<int8_t>("-12345", std::nullopt);
    InnerTestSingleAttribute<int8_t>("1.0", std::nullopt);
    InnerTestSingleAttribute<uint8_t>("-1", std::nullopt);
    InnerTestSingleAttribute<uint64_t>("fuck", std::nullopt);
    InnerTestSingleAttribute<uint64_t>("", 0);
    InnerTestSingleAttribute<uint32_t, ft_time>("", 0);
    InnerTestSingleAttribute<uint32_t, ft_date>("", 0);
    InnerTestSingleAttribute<uint64_t, ft_timestamp>("", 0);

    InnerTestSingleAttribute<uint32_t, ft_time>("02:13:14.326", 7994326);
    InnerTestSingleAttribute<uint32_t, ft_date>("1992-02-22", 8087);
    InnerTestSingleAttribute<uint64_t, ft_timestamp>("1991-02-03 13:00:00.001", 665586000001);
}

TEST_F(DefaultValueAttributePatchTest, testSingleAttributeWithPatch)
{
    InnerTestSingleAttributeWithPatch<int8_t>("-12", -12, {{1, 1}, {2, 2}, {3, -3}, {100, -100}});
    InnerTestSingleAttributeWithPatch<uint8_t>("1", 1, {{1, 2}, {2, 3}});
    InnerTestSingleAttributeWithPatch<int16_t>("345", 345, {{9, 1}, {3, 4}});
    InnerTestSingleAttributeWithPatch<uint16_t>("213", 213, {{1, 1}});
    InnerTestSingleAttributeWithPatch<int32_t>("30000", 30000, {{1, 1}});
    InnerTestSingleAttributeWithPatch<uint32_t>("65535", 65535, {{1, 1}});
    InnerTestSingleAttributeWithPatch<int64_t>("-12300000", -12300000, {{1, 1}});
    InnerTestSingleAttributeWithPatch<uint64_t>("123456789", 123456789, {{1, 1}});
    InnerTestSingleAttributeWithPatch<float>("12.3", 12.3, {{1, 5.1}});
    InnerTestSingleAttributeWithPatch<double>("123.4", 123.4, {{1, 2314.1}});
}

TEST_F(DefaultValueAttributePatchTest, testMultiAttribute)
{
    InnerTestMultiAttribute<int8_t>({"-12"}, std::vector<int8_t> {-12}, std::nullopt);
    InnerTestMultiAttribute<int8_t>({"-12", "123"}, std::vector<int8_t> {-12, 123}, std::nullopt);
    InnerTestMultiAttribute<int8_t>({"-12", "123"}, std::vector<int8_t> {-12, 123}, 2);
    InnerTestMultiAttribute<int8_t>({}, std::vector<int8_t> {}, std::nullopt);
    InnerTestMultiAttribute<int64_t>({"", "123"}, std::vector<int64_t> {123}, std::nullopt);

    InnerTestMultiAttribute<uint8_t>({"12", "123"}, std::vector<uint8_t> {12, 123}, std::nullopt);
    InnerTestMultiAttribute<int16_t>({"12", "123"}, std::vector<int16_t> {12, 123}, std::nullopt);
    InnerTestMultiAttribute<uint16_t>({"12", "123"}, std::vector<uint16_t> {12, 123}, std::nullopt);
    InnerTestMultiAttribute<int32_t>({"12", "123"}, std::vector<int32_t> {12, 123}, std::nullopt);
    InnerTestMultiAttribute<uint32_t>({"12", "123"}, std::vector<uint32_t> {12, 123}, std::nullopt);
    InnerTestMultiAttribute<int64_t>({"12", "123"}, std::vector<int64_t> {12, 123}, std::nullopt);
    InnerTestMultiAttribute<uint64_t>({"12", "123"}, std::vector<uint64_t> {12, 123}, std::nullopt);
    InnerTestMultiAttribute<float>({"1.2", "12.3"}, std::vector<float> {1.2, 12.3}, std::nullopt);
    InnerTestMultiAttribute<double>({"1.2", "12.3"}, std::vector<double> {1.2, 12.3}, std::nullopt);

    InnerTestMultiAttribute<int8_t>({"-12", "123"}, std::nullopt, 1);
    InnerTestMultiAttribute<int8_t>({"-12", "23123"}, std::nullopt, std::nullopt);
    InnerTestMultiAttribute<uint8_t>({"-12", "123"}, std::nullopt, std::nullopt);
    InnerTestMultiAttribute<uint8_t>({"fuck", "123"}, std::nullopt, std::nullopt);
    InnerTestMultiAttribute<int8_t>({}, std::vector<int8_t> {0}, 1);
}

TEST_F(DefaultValueAttributePatchTest, testStringAttribute)
{
    auto InnerStringAttribute = [](std::vector<std::string> values) {
        std::string str;
        if (!values.empty()) {
            for (auto valueStr : values) {
                str += valueStr + INDEXLIB_MULTI_VALUE_SEPARATOR_STR;
            }
            str.pop_back();
        }
        auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>("test", ft_string, values.size() > 1);
        fieldConfig->SetDefaultValue(str);
        auto attrConfig = std::make_shared<AttributeConfig>();
        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
        DefaultValueAttributePatch patch;
        ASSERT_TRUE(patch.Open(attrConfig).IsOK());

        uint8_t buffer[1024];
        bool isNull;
        auto [status, length] = patch.Seek(0, buffer, sizeof(buffer), isNull);
        ASSERT_TRUE(status.IsOK());
        AttributeFieldPrinter printer;
        printer.Init(fieldConfig);
        std::string output;

        if (values.size() == 1) {
            autil::MultiValueType<char> value(buffer);
            ASSERT_TRUE(printer.Print(isNull, value, &output));
        } else {
            autil::MultiValueType<autil::MultiChar> value(buffer);
            ASSERT_TRUE(printer.Print(isNull, value, &output));
        }
        ASSERT_EQ(str, output);
    };
    InnerStringAttribute({"default"});
    InnerStringAttribute({"a", "b", "cdf"});
}

} // namespace indexlibv2::index
