#include "indexlib/index/attribute/format/SingleValueAttributeMemFormatter.h"

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

class SingleValueAttributeMemFormatterTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    SingleValueAttributeMemFormatterTest();
    ~SingleValueAttributeMemFormatterTest();

    DECLARE_CLASS_NAME(SingleValueAttributeMemFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestUpdateValue();

private:
    template <typename T>
    void InnerTest(std::shared_ptr<AttributeConfig> attrConfig, size_t count, bool hasUpdate = false,
                   bool needCheckValue = true)
    {
        std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
        std::shared_ptr<AttributeConvertor> attrConvertor(new SingleValueAttributeConvertor<T>(false, "test"));
        SingleValueAttributeMemFormatter<T>* formatter = new SingleValueAttributeMemFormatter<T>(attrConfig);
        formatter->Init(pool, attrConvertor);
        std::vector<T> value;
        CreateData(formatter, count, value);
        CheckData(formatter, value, needCheckValue);
        if (hasUpdate) {
            UpdateData(formatter, value, pool.get());
            CheckData(formatter, value, needCheckValue, hasUpdate);
        }
        delete formatter;
    }

    template <typename T>
    void CreateData(SingleValueAttributeMemFormatter<T>* formatter, size_t count, std::vector<T>& values)
    {
        for (size_t i = 0; i < count; i++) {
            T value = i % 10 == 0 ? (rand() % 128) : std::numeric_limits<T>::max();
            if (i % NULL_DOC == 0) {
                formatter->AddField(i, value, true);
            } else {
                formatter->AddField(i, value, false);
            }
            values.push_back(value);
        }
    }

    template <typename T>
    void UpdateData(SingleValueAttributeMemFormatter<T>* formatter, std::vector<T>& values,
                    autil::mem_pool::PoolBase* pool)
    {
        for (size_t i = 0; i < values.size(); i++) {
            if (i % NULL_DOC == 0) {
                T value = i % 10 == 0 ? (rand() % 128) : std::numeric_limits<T>::max();
                char* data = (char*)pool->allocate(sizeof(T));
                memcpy(data, &value, sizeof(T));
                formatter->UpdateField(i, autil::StringView(data, sizeof(T)), false);
                values[i] = value;
            }
            if (i % UPDATE_NULL_DOC == 0) {
                formatter->UpdateField(i, autil::StringView::empty_instance(), true);
            }
        }
    }

    template <typename T>
    void CheckData(SingleValueAttributeMemFormatter<T>* formatter, std::vector<T>& values, bool needCheckValue,
                   bool isUpdated = false)
    {
        int nullDoc = isUpdated ? UPDATE_NULL_DOC : NULL_DOC;
        for (size_t i = 0; i < values.size(); i++) {
            T value;
            bool isNull;
            formatter->Read(i, value, isNull);
            if (i % nullDoc == 0) {
                EXPECT_TRUE(isNull) << "docid " << i << std::endl;
            } else {
                EXPECT_FALSE(isNull) << "docid " << i << std::endl;
                if (needCheckValue) {
                    EXPECT_EQ(values[i], value) << "docid " << i << std::endl;
                }
            }
        }
    }

private:
    const int64_t NULL_DOC = 3;
    const int64_t UPDATE_NULL_DOC = 4;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleValueAttributeMemFormatterTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.index, SingleValueAttributeMemFormatterTest);

SingleValueAttributeMemFormatterTest::SingleValueAttributeMemFormatterTest() {}

SingleValueAttributeMemFormatterTest::~SingleValueAttributeMemFormatterTest() {}

void SingleValueAttributeMemFormatterTest::CaseSetUp() {}

void SingleValueAttributeMemFormatterTest::CaseTearDown() {}

void SingleValueAttributeMemFormatterTest::TestSimpleProcess()
{
#define TEST_FORMATTER(type, ft_type)                                                                                  \
    {                                                                                                                  \
        std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(                                                  \
            new indexlibv2::config::FieldConfig("test", ft_type, false));                                              \
        std::shared_ptr<AttributeConfig> attrConfig(new index::AttributeConfig());                                     \
        ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());                                                             \
        attrConfig->GetFieldConfig()->SetEnableNullField(true);                                                        \
        InnerTest<type>(attrConfig, 0);                                                                                \
        InnerTest<type>(attrConfig, 64);                                                                               \
        InnerTest<type>(attrConfig, 321);                                                                              \
    }
    TEST_FORMATTER(uint8_t, ft_uint8);
    TEST_FORMATTER(int8_t, ft_int8);
    TEST_FORMATTER(uint16_t, ft_uint16);
    TEST_FORMATTER(int16_t, ft_int16);
    TEST_FORMATTER(uint32_t, ft_uint32);
    TEST_FORMATTER(int32_t, ft_int32);
    TEST_FORMATTER(uint64_t, ft_uint64);
    TEST_FORMATTER(int64_t, ft_int64);
    TEST_FORMATTER(float, ft_float);
    TEST_FORMATTER(double, ft_double);
#undef TEST_FORMATTER
}

void SingleValueAttributeMemFormatterTest::TestUpdateValue()
{
#define TEST_UPDATE_FORMATTER(type, ft_type)                                                                           \
    {                                                                                                                  \
        std::shared_ptr<indexlibv2::config::FieldConfig> fieldConfig(                                                  \
            new indexlibv2::config::FieldConfig("test", ft_type, false));                                              \
        std::shared_ptr<AttributeConfig> attrConfig(new index::AttributeConfig());                                     \
        auto status = attrConfig->Init(fieldConfig);                                                                   \
        ASSERT_TRUE(status.IsOK());                                                                                    \
        attrConfig->GetFieldConfig()->SetEnableNullField(true);                                                        \
        InnerTest<type>(attrConfig, 0, true);                                                                          \
        InnerTest<type>(attrConfig, 64, true);                                                                         \
        InnerTest<type>(attrConfig, 513, true);                                                                        \
    }
    TEST_UPDATE_FORMATTER(uint8_t, ft_uint8);
    TEST_UPDATE_FORMATTER(int8_t, ft_int8);
    TEST_UPDATE_FORMATTER(uint16_t, ft_uint16);
    TEST_UPDATE_FORMATTER(int16_t, ft_int16);
    TEST_UPDATE_FORMATTER(uint32_t, ft_uint32);
    TEST_UPDATE_FORMATTER(int32_t, ft_int32);
    TEST_UPDATE_FORMATTER(uint64_t, ft_uint64);
    TEST_UPDATE_FORMATTER(int64_t, ft_int64);
    TEST_UPDATE_FORMATTER(float, ft_float);
    TEST_UPDATE_FORMATTER(double, ft_double);
#undef TEST_UPDATE_FORMATTER
}
}} // namespace indexlibv2::index
