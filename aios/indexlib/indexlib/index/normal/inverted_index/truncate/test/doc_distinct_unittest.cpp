#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_distinctor.h"
#include "indexlib/index/normal/inverted_index/truncate/test/fake_truncate_attribute_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class DocDistinctTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DocDistinctTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    template<typename T>
    void InnerTestCaseForDistinct()
    {
        std::string attrValue = "1;2;2;3;4;4;5";
        FakeTruncateAttributeReader<T>* pAttrReader = new FakeTruncateAttributeReader<T>();
        pAttrReader->SetAttrValue(attrValue);
        TruncateAttributeReaderPtr attrReader(pAttrReader);
        DocDistinctorTyped<T> docDistinctTyped(attrReader, 4);
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(0));
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(1));
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(2));
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(3));
        INDEXLIB_TEST_TRUE(docDistinctTyped.Distinct(4));
        INDEXLIB_TEST_TRUE(docDistinctTyped.Distinct(5));
        INDEXLIB_TEST_TRUE(docDistinctTyped.Distinct(5));
    }

    void TestCaseForDistinct()
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


private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, DocDistinctTest);

INDEXLIB_UNIT_TEST_CASE(DocDistinctTest, TestCaseForDistinct);

IE_NAMESPACE_END(index);
