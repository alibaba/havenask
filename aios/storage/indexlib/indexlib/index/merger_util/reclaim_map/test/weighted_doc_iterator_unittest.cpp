#include <queue>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/reclaim_map/weighted_doc_iterator.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib::index_base;
using indexlib::index::legacy::WeightedDocIterator;
using indexlib::index::legacy::WeightedDocIteratorComp;

namespace indexlib { namespace index {

class WeightedDocIteratorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(WeightedDocIteratorTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForSimple()
    {
        CheckResult("1,2", "1,2", false);
        CheckResult("1,2", "2,1", true);
        CheckResult("1,2,10,5,8", "10,8,5,2,1", true);
        CheckResult("10,5,8,1,2", "1,2,5,8,10", false);

        CheckResult("1,2,10,5,8", "8,5,1,2,10", true, "", "", true, "false,true,true,false,false");
        CheckResult("10,5,8,1,2", "10,2,1,5,8", false, "", "", true, "true,false,false,false,true");
    }

    void TestCaseForSegment()
    {
        CheckResult("1,2,2,2", "1,2,2,2", false, "0,1,2,3", "0,1,2,3");
        CheckResult("1,2,2,2", "2,2,2,1", true, "0,1,2,3", "1,2,3,0");
    }

private:
    void CheckResult(string valueStr, string expectValueStr, bool desc = false, const string& SegIdStr = "",
                     const string& expectSegIdStr = "", bool supportNull = false, const string& nullVecStr = "")
    {
        vector<int32_t> value;
        vector<int32_t> expectValue;
        vector<segmentid_t> segmentId;
        vector<segmentid_t> expectSegmentId;
        vector<bool> nullVec;
        StringUtil::fromString(valueStr, value, ",");
        StringUtil::fromString(expectValueStr, expectValue, ",");
        StringUtil::fromString(SegIdStr, segmentId, ",");
        StringUtil::fromString(expectSegIdStr, expectSegmentId, ",");
        StringUtil::fromString(nullVecStr, nullVec, ",");
        if (nullVec.size() == 0) {
            nullVec.resize(value.size(), false);
        }

        legacy::DocInfoAllocatorPtr allocator(new legacy::DocInfoAllocator);
        legacy::ReferenceTyped<int32_t>* refer =
            allocator->DeclareReferenceTyped<int32_t>("test", ft_int32, supportNull);
        legacy::ComparatorTyped<int32_t>* comp = new legacy::ComparatorTyped<int32_t>(refer, desc);
        legacy::ComparatorPtr comparator(comp);
        priority_queue<WeightedDocIterator*, vector<WeightedDocIterator*>, WeightedDocIteratorComp> docHeap;
        for (size_t i = 0; i < value.size(); ++i) {
            SegmentMergeInfo segmentMergeInfo;
            if (segmentId.size() > 0) {
                segmentMergeInfo.segmentId = segmentId[i];
            }
            WeightedDocIterator* iter =
                new WeightedDocIterator(segmentMergeInfo, index::legacy::EvaluatorPtr(), comparator, allocator);

            legacy::DocInfo* docinfo = iter->GetDocInfo();
            refer->Set(value[i], nullVec[i], docinfo);
            docHeap.push(iter);
        }

        size_t nullCount = 0;
        for (size_t i = 0; i < nullVec.size(); ++i) {
            if (nullVec[i]) {
                nullCount++;
            }
        }
        if (!desc) {
            // if asc, null value should be iterated first
            for (size_t i = 0; i < nullCount; ++i) {
                WeightedDocIterator* iter = docHeap.top();
                docHeap.pop();
                legacy::DocInfo* docinfo = iter->GetDocInfo();
                int32_t actualValue = 0;
                bool isNull = false;
                refer->Get(docinfo, actualValue, isNull);
                ASSERT_TRUE(isNull);
                delete iter;
            }
        }
        size_t begin = desc ? 0 : nullCount;
        size_t end = desc ? expectValue.size() - nullCount : expectValue.size();
        for (; begin < end; begin++) {
            WeightedDocIterator* iter = docHeap.top();
            docHeap.pop();
            legacy::DocInfo* docinfo = iter->GetDocInfo();
            int32_t actualValue = 0;
            bool isNull = false;
            refer->Get(docinfo, actualValue, isNull);
            INDEXLIB_TEST_EQUAL(expectValue[begin], actualValue);
            ASSERT_FALSE(isNull);
            if (expectSegmentId.size() > 0) {
                INDEXLIB_TEST_EQUAL(expectSegmentId[begin], iter->GetSegmentId());
            }
            delete iter;
        }
        if (desc) {
            // if desc, null value should be iterated last
            for (size_t i = 0; i < nullCount; ++i) {
                WeightedDocIterator* iter = docHeap.top();
                docHeap.pop();
                legacy::DocInfo* docinfo = iter->GetDocInfo();
                int32_t actualValue = 0;
                bool isNull = false;
                refer->Get(docinfo, actualValue, isNull);
                ASSERT_TRUE(isNull);
                delete iter;
            }
        }
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, WeightedDocIteratorTest);

INDEXLIB_UNIT_TEST_CASE(WeightedDocIteratorTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(WeightedDocIteratorTest, TestCaseForSegment);
}} // namespace indexlib::index
