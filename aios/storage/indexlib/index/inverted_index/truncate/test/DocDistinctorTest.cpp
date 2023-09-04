#include "indexlib/index/inverted_index/truncate/DocDistinctor.h"

#include "indexlib/index/inverted_index/truncate/test/FakeTruncateAttributeReader.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class DocDistinctorTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}

private:
    template <typename T>
    void InnerTestCaseForDistinct();
};

template <typename T>
void DocDistinctorTest::InnerTestCaseForDistinct()
{
    std::shared_ptr<indexlibv2::index::DocMapper> docMapper;
    auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
    auto pAttrReader = std::make_shared<FakeTruncateAttributeReader<T>>(docMapper, attrConfig);

    std::string attrValue = "1;2;2;3;4;4;5";
    std::string nullVec = "false;false;fasle;false;true;false;false";
    pAttrReader->SetAttrValue(attrValue, nullVec);
    auto docDistinctTyped = std::make_shared<DocDistinctorTyped<T>>(pAttrReader, 4);

    ASSERT_TRUE(!docDistinctTyped->Distinct(0));
    ASSERT_FALSE(docDistinctTyped->IsFull());

    ASSERT_TRUE(!docDistinctTyped->Distinct(1));
    ASSERT_FALSE(docDistinctTyped->IsFull());

    ASSERT_TRUE(!docDistinctTyped->Distinct(2));
    ASSERT_FALSE(docDistinctTyped->IsFull());

    ASSERT_TRUE(!docDistinctTyped->Distinct(3));
    ASSERT_FALSE(docDistinctTyped->IsFull());

    // doc 4 is null
    ASSERT_TRUE(!docDistinctTyped->Distinct(4));
    ASSERT_FALSE(docDistinctTyped->IsFull());

    ASSERT_TRUE(docDistinctTyped->Distinct(5));
    ASSERT_TRUE(docDistinctTyped->IsFull());

    ASSERT_TRUE(docDistinctTyped->Distinct(5));
    ASSERT_TRUE(docDistinctTyped->IsFull());

    ASSERT_TRUE(docDistinctTyped->Distinct(6));
    ASSERT_TRUE(docDistinctTyped->IsFull());
}

TEST_F(DocDistinctorTest, TestDistinct)
{
    InnerTestCaseForDistinct<uint8_t>();
    InnerTestCaseForDistinct<int8_t>();
    InnerTestCaseForDistinct<uint16_t>();
    InnerTestCaseForDistinct<int16_t>();
    InnerTestCaseForDistinct<uint32_t>();
    InnerTestCaseForDistinct<int32_t>();
    InnerTestCaseForDistinct<uint64_t>();
    InnerTestCaseForDistinct<int64_t>();
}

} // namespace indexlib::index
