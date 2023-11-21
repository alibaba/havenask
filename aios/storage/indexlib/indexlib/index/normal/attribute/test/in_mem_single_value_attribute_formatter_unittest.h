#pragma once

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/single_value_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/DefaultFSWriterParamDecider.h"
#include "indexlib/index/normal/attribute/format/in_mem_single_value_attribute_formatter.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class InMemSingleValueAttributeFormatterTest : public INDEXLIB_TESTBASE
{
public:
    InMemSingleValueAttributeFormatterTest();
    ~InMemSingleValueAttributeFormatterTest();

    DECLARE_CLASS_NAME(InMemSingleValueAttributeFormatterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestUpdateValue();

private:
    template <typename T>
    void InnerTest(config::AttributeConfigPtr attrConfig, size_t count, bool hasUpdate = false,
                   bool needCheckValue = true)
    {
        FSWriterParamDeciderPtr fsWriterParamDecider(new DefaultFSWriterParamDecider());
        std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
        InMemSingleValueAttributeFormatter<T>* formatter = new InMemSingleValueAttributeFormatter<T>(attrConfig);
        formatter->Init(fsWriterParamDecider, pool);
        common::AttributeConvertorPtr attrConvertor(new common::SingleValueAttributeConvertor<T>(false, "test"));
        formatter->SetAttrConvertor(attrConvertor);
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
    void CreateData(InMemSingleValueAttributeFormatter<T>* formatter, size_t count, std::vector<T>& values)
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
    void UpdateData(InMemSingleValueAttributeFormatter<T>* formatter, std::vector<T>& values,
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
    void CheckData(InMemSingleValueAttributeFormatter<T>* formatter, std::vector<T>& values, bool needCheckValue,
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
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemSingleValueAttributeFormatterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(InMemSingleValueAttributeFormatterTest, TestUpdateValue);
}} // namespace indexlib::index
