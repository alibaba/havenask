#include "indexlib/index/attribute/format/SingleValueNullAttributeUpdatableFormatter.h"

#include "autil/mem_pool/Pool.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace index {

class SingleValueNullAttributeUpdatableFormatterTest : public TESTBASE
{
public:
    SingleValueNullAttributeUpdatableFormatterTest() = default;
    ~SingleValueNullAttributeUpdatableFormatterTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}

private:
    template <typename T>
    void InnerTest(SingleValueNullAttributeUpdatableFormatter<T>* formatter, size_t count, bool needCheckValue = true)
    {
        formatter->Init(indexlib::config::CompressTypeOption());
        autil::mem_pool::Pool pool;
        void* data = pool.allocate(SingleValueNullAttributeUpdatableFormatter<T>::GetDataLen(count));
        std::vector<T> value;
        CreateData(formatter, count, value, data);
        CheckData(formatter, value, data, needCheckValue);
    }

    template <typename T>
    void CreateData(SingleValueNullAttributeUpdatableFormatter<T>* formatter, size_t count, std::vector<T>& values,
                    void* data)
    {
        for (size_t i = 0; i < count; i++) {
            T value = i % 10 == 0 ? (rand() % 128) : std::numeric_limits<T>::max();
            if (i % NULL_DOC == 0) {
                formatter->Set(i, (uint8_t*)data, value, true);
            } else {
                formatter->Set(i, (uint8_t*)data, value, false);
            }
            values.push_back(value);
        }
    }

    template <typename T>
    void CheckData(SingleValueNullAttributeUpdatableFormatter<T>* formatter, std::vector<T>& values, void* data,
                   bool needCheckValue)
    {
        for (size_t i = 0; i < values.size(); i++) {
            T value;
            bool isNull;
            ASSERT_TRUE(formatter->Get(i, (uint8_t*)data, value, isNull).IsOK());
            if (i % NULL_DOC == 0) {
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
};

TEST_F(SingleValueNullAttributeUpdatableFormatterTest, TestSimpleProcess)
{
#define TEST_FORMATTER(type)                                                                                           \
    {                                                                                                                  \
        SingleValueNullAttributeUpdatableFormatter<type>* formatter =                                                  \
            new SingleValueNullAttributeUpdatableFormatter<type>();                                                    \
        InnerTest<type>(formatter, 0);                                                                                 \
        InnerTest<type>(formatter, 64);                                                                                \
        InnerTest<type>(formatter, 321);                                                                               \
        delete formatter;                                                                                              \
    }

    TEST_FORMATTER(uint8_t);
    TEST_FORMATTER(int8_t);
    TEST_FORMATTER(uint16_t);
    TEST_FORMATTER(int16_t);
    TEST_FORMATTER(uint32_t);
    TEST_FORMATTER(int32_t);
    TEST_FORMATTER(uint64_t);
    TEST_FORMATTER(int64_t);
    TEST_FORMATTER(float);
    TEST_FORMATTER(double);
    TEST_FORMATTER(char);
    TEST_FORMATTER(bool);
#undef TEST_FORMATTER
}

TEST_F(SingleValueNullAttributeUpdatableFormatterTest, TestForFloatCompress)
{
    {
        SingleValueNullAttributeUpdatableFormatter<float>* formatter =
            new SingleValueNullAttributeUpdatableFormatter<float>();
        indexlib::config::CompressTypeOption compressType;
        ASSERT_TRUE(compressType.Init("fp16").IsOK());
        formatter->Init(compressType);
        InnerTest<float>(formatter, 0, false);
        InnerTest<float>(formatter, 64, false);
        InnerTest<float>(formatter, 321, false);
        delete formatter;
    }
    {
        SingleValueNullAttributeUpdatableFormatter<float>* formatter =
            new SingleValueNullAttributeUpdatableFormatter<float>();
        indexlib::config::CompressTypeOption compressType;
        ASSERT_TRUE(compressType.Init("int8#200").IsOK());
        formatter->Init(compressType);
        InnerTest<float>(formatter, 0, false);
        InnerTest<float>(formatter, 64, false);
        InnerTest<float>(formatter, 321, false);
        delete formatter;
    }
}

}} // namespace indexlibv2::index
