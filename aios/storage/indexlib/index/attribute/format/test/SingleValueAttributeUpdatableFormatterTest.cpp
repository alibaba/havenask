#include "indexlib/index/attribute/format/SingleValueAttributeUpdatableFormatter.h"

#include "autil/StringUtil.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlibv2 { namespace index {

class SingleValueAttributeUpdatableFormatterTest : public TESTBASE
{
public:
    SingleValueAttributeUpdatableFormatterTest() {}
    ~SingleValueAttributeUpdatableFormatterTest() {}

public:
    void setUp() override {}
    void tearDown() override {}

    template <typename T>
    void InnerTestForMemBuffer(uint32_t docNum)
    {
        typedef shared_ptr<SingleValueAttributeConvertor<T>> SingleValueAttributeConvertorPtr;
        SingleValueAttributeUpdatableFormatter<T> formatter(indexlib::config::CompressTypeOption(), false);
        formatter.SetAttrConvertor(SingleValueAttributeConvertorPtr(new SingleValueAttributeConvertor<T>()));

        uint8_t* data = new uint8_t[sizeof(T) * docNum];
        vector<T> expectedData;
        AddData<T>(data, formatter, expectedData, docNum);
        CheckData<T>(data, formatter, expectedData);
        delete[] data;
    }

    template <typename T>
    void AddData(uint8_t* data, SingleValueAttributeUpdatableFormatter<T>& formatter, vector<T>& expectedData,
                 uint32_t docNum)
    {
        expectedData.resize(docNum);
        T value;
        for (uint32_t i = 0; i < (docNum - 1); ++i) {
            value = i * 3 % 10;
            expectedData[i] = value;
            formatter.Set((docid_t)i, data, value, (i % NULL_DOC) == 0);
        }
        // last doc for empty value
        expectedData[docNum - 1] = 0;
        value = 0;
        formatter.Set((docid_t)(docNum - 1), data, value, ((docNum - 1) % NULL_DOC) == 0);
    }

    template <typename T>
    void CheckData(uint8_t* data, SingleValueAttributeUpdatableFormatter<T>& formatter, vector<T>& expectedData)
    {
        T value;
        bool isNull;
        for (uint32_t i = 0; i < expectedData.size(); ++i) {
            ASSERT_TRUE(formatter.Get((docid_t)i, data, value, isNull).IsOK());
            ASSERT_EQ(expectedData[i], value);
        }
    }

private:
    autil::mem_pool::Pool mPool;
    const int32_t NULL_DOC = 3;
};

TEST_F(SingleValueAttributeUpdatableFormatterTest, TestCaseForSimple)
{
    InnerTestForMemBuffer<uint64_t>(500);
    InnerTestForMemBuffer<double>(250);
}

TEST_F(SingleValueAttributeUpdatableFormatterTest, TestCompressedFloat)
{
    indexlib::config::CompressTypeOption compress;
    ASSERT_TRUE(compress.Init("fp16").IsOK());

    SingleValueAttributeUpdatableFormatter<float> formatter(compress, false);

    char data[1024];
    float value1 = 1.1, value2 = 1.2, value3 = -1.1;
    float buf1, buf2, buf3;
    Fp16Encoder::Encode(value1, (char*)&buf1);
    Fp16Encoder::Encode(value2, (char*)&buf2);
    Fp16Encoder::Encode(value3, (char*)&buf3);
    formatter.Set(0, (uint8_t*)data, buf1, /* isNull */ false);
    formatter.Set(1, (uint8_t*)data, buf2, /* isNull */ false);
    formatter.Set(2, (uint8_t*)data, buf3, /* isNull */ false);

    float output1, output2, output3;
    bool isNull;
    ASSERT_TRUE(formatter.Get(0, (uint8_t*)data, output1, isNull).IsOK());
    ASSERT_TRUE(formatter.Get(1, (uint8_t*)data, output2, isNull).IsOK());
    ASSERT_TRUE(formatter.Get(2, (uint8_t*)data, output3, isNull).IsOK());
    EXPECT_FLOAT_EQ(1.0996094, output1);
    EXPECT_FLOAT_EQ(1.1992188, output2);
    EXPECT_FLOAT_EQ(-1.0996094, output3);
}

}} // namespace indexlibv2::index
