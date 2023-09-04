#ifndef __INDEXLIB_SINGLEVALUENULLATTRFORMATTERTEST_H
#define __INDEXLIB_SINGLEVALUENULLATTRFORMATTERTEST_H

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/format/single_value_null_attr_formatter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SingleValueNullAttrFormatterTest : public INDEXLIB_TESTBASE
{
public:
    SingleValueNullAttrFormatterTest();
    ~SingleValueNullAttrFormatterTest();

    DECLARE_CLASS_NAME(SingleValueNullAttrFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestForFloatCompress();

private:
    template <typename T>
    void InnerTest(SingleValueNullAttrFormatter<T>* formatter, size_t count, bool needCheckValue = true)
    {
        formatter->Init();
        autil::mem_pool::Pool pool;
        void* data = pool.allocate(SingleValueNullAttrFormatter<T>::GetDataLen(count));
        std::vector<T> value;
        CreateData(formatter, count, value, data);
        CheckData(formatter, value, data, needCheckValue);
    }

    template <typename T>
    void CreateData(SingleValueNullAttrFormatter<T>* formatter, size_t count, std::vector<T>& values, void* data)
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
    void CheckData(SingleValueNullAttrFormatter<T>* formatter, std::vector<T>& values, void* data, bool needCheckValue)
    {
        for (size_t i = 0; i < values.size(); i++) {
            T value;
            bool isNull;
            formatter->Get(i, (uint8_t*)data, value, isNull);
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

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleValueNullAttrFormatterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SingleValueNullAttrFormatterTest, TestForFloatCompress);
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLEVALUENULLATTRFORMATTERTEST_H
