#include "indexlib/index/attribute/SingleValueAttributeMemReader.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/util/slice_array/AlignedSliceArray.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlibv2::config;

namespace indexlibv2::index {

class SingleValueAttributeMemReaderTest : public TESTBASE
{
public:
    SingleValueAttributeMemReaderTest() = default;
    ~SingleValueAttributeMemReaderTest() = default;

public:
    void setUp() override { srand(8888); }
    void tearDown() override {}

private:
    void BuildData(indexlib::index::TypedSliceList<uint32_t>& data, vector<uint32_t>& answer);

private:
    autil::mem_pool::Pool _pool;
};

void SingleValueAttributeMemReaderTest::BuildData(indexlib::index::TypedSliceList<uint32_t>& data,
                                                  vector<uint32_t>& answer)
{
    for (uint32_t i = 0; i < 200; ++i) {
        uint32_t value = rand() % 100;
        data.PushBack(value);
        answer.push_back(value);
    }
}

TEST_F(SingleValueAttributeMemReaderTest, TestCaseForRead)
{
    indexlib::index::TypedSliceList<uint32_t> data(4, 2, &_pool);
    vector<uint32_t> answer;
    BuildData(data, answer);
    std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig());
    std::shared_ptr<FieldConfig> fieldConfig(new FieldConfig("test", ft_uint32, false));
    ASSERT_TRUE(attrConfig->Init(fieldConfig).IsOK());
    SingleValueAttributeMemFormatter<uint32_t> formatter(attrConfig);
    formatter._data = &data;
    formatter._supportNull = false;
    SingleValueAttributeMemReader<uint32_t> reader(&formatter, indexlib::config::CompressTypeOption());
    ASSERT_TRUE(reader.IsInMemory());
    for (size_t docId = 0; docId < answer.size(); ++docId) {
        uint32_t value;
        bool isNull = false;
        ASSERT_EQ(sizeof(uint32_t), reader.TEST_GetDataLength((docId)));
        reader.Read((uint64_t)docId, value, isNull, nullptr);
        ASSERT_EQ(answer[docId], value);
    }
}

} // namespace indexlibv2::index
