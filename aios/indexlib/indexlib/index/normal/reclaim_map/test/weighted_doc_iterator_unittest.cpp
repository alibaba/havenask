#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/reclaim_map/weighted_doc_iterator.h"
#include <autil/StringUtil.h>
#include <queue>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);

class WeightedDocIteratorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(WeightedDocIteratorTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForSimple()
    {
        CheckResult("1,2", "1,2", false);
        CheckResult("1,2", "2,1", true);
        CheckResult("1,2,10,5,8", "10,8,5,2,1", true);
        CheckResult("10,5,8,1,2", "1,2,5,8,10", false);
    }

    void TestCaseForSegment()
    {
        CheckResult("1,2,2,2", "1,2,2,2", false, "0,1,2,3", "0,1,2,3");
        CheckResult("1,2,2,2", "2,2,2,1", true, "0,1,2,3", "1,2,3,0");
    }


private:
    void CheckResult(string valueStr, string expectValueStr, bool desc=false,
                     string SegIdStr = "", string expectSegIdStr = "")
    {
        vector<int32_t> value;
        vector<int32_t> expectValue;
        vector<segmentid_t> segmentId;
        vector<segmentid_t> expectSegmentId;
        StringUtil::fromString(valueStr, value, ",");
        StringUtil::fromString(expectValueStr, expectValue, ",");
        StringUtil::fromString(SegIdStr, segmentId, ",");
        StringUtil::fromString(expectSegIdStr, expectSegmentId, ",");

        DocInfoAllocatorPtr allocator(new DocInfoAllocator);
        ReferenceTyped<int32_t>* refer = allocator->DeclareReferenceTyped<int32_t>(
                "test", ft_int32);
        ComparatorTyped<int32_t>* comp = new ComparatorTyped<int32_t>(refer, desc);
        ComparatorPtr comparator(comp);
        priority_queue<WeightedDocIterator*, vector<WeightedDocIterator*>,
                       WeightedDocIteratorComp> docHeap;
        for (size_t i = 0; i < value.size(); ++i)
        {
            SegmentMergeInfo segmentMergeInfo;
            if (segmentId.size() > 0)
            {
                segmentMergeInfo.segmentId = segmentId[i];
            }
            WeightedDocIterator* iter = new WeightedDocIterator(segmentMergeInfo,
                            EvaluatorPtr(), comparator, allocator);

            DocInfo *docinfo = iter->GetDocInfo();
            refer->Set(value[i], docinfo);
            docHeap.push(iter);
        }

        for (size_t i = 0; i < expectValue.size(); ++i)
        {
            WeightedDocIterator* iter = docHeap.top();
            docHeap.pop();
            DocInfo* docinfo = iter->GetDocInfo();
            int32_t actualValue = 0;
            refer->Get(docinfo, actualValue);
            INDEXLIB_TEST_EQUAL(expectValue[i], actualValue);
            if (expectSegmentId.size() > 0)
            {
                INDEXLIB_TEST_EQUAL(expectSegmentId[i], iter->GetSegmentId());                
            }
            delete iter;
        }
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, WeightedDocIteratorTest);

INDEXLIB_UNIT_TEST_CASE(WeightedDocIteratorTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(WeightedDocIteratorTest, TestCaseForSegment);

IE_NAMESPACE_END(index);
