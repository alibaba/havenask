#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/doc_distinctor.h"
#include "indexlib/index/merger_util/truncate/test/fake_truncate_attribute_reader.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib::index::legacy {

class DocDistinctTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DocDistinctTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    template <typename T>
    void InnerTestCaseForDistinct()
    {
        std::string attrValue = "1;2;2;3;4;4;5";
        FakeTruncateAttributeReader<T>* pAttrReader = new FakeTruncateAttributeReader<T>();
        string nullVec = "false;false;fasle;false;true;false;false";
        pAttrReader->SetAttrValue(attrValue, nullVec);
        TruncateAttributeReaderPtr attrReader(pAttrReader);
        DocDistinctorTyped<T> docDistinctTyped(attrReader, 4);
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(0));
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(1));
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(2));
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(3));
        INDEXLIB_TEST_TRUE(!docDistinctTyped.Distinct(4));
        INDEXLIB_TEST_TRUE(docDistinctTyped.Distinct(5));
        INDEXLIB_TEST_TRUE(docDistinctTyped.Distinct(5));
        INDEXLIB_TEST_TRUE(docDistinctTyped.Distinct(6));
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
} // namespace indexlib::index::legacy
