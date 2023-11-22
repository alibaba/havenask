#include "indexlib/common_define.h"
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/config/truncate_index_config.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/merger_util/truncate/doc_payload_filter_processor.h"
#include "indexlib/test/unittest.h"

using namespace std;

using namespace indexlib::config;

namespace indexlib::index::legacy {

class FakeTruncatePostingIterator : public PostingIterator
{
public:
    FakeTruncatePostingIterator() { mCurIdx = 0; };
    ~FakeTruncatePostingIterator() {};

public:
    TermMeta* GetTermMeta() const override { return NULL; };
    docid64_t SeekDoc(docid64_t docId) override { return 0; };
    bool HasPosition() const override { return true; };
    void Unpack(TermMatchData& termMatchData) override {};
    docpayload_t GetDocPayload() override
    {
        assert((size_t)mCurIdx < mDocPayloads.size());
        return mDocPayloads[mCurIdx++];
    }

    void SetDocPayload(std::string payloads) { autil::StringUtil::fromString(payloads, mDocPayloads, ";"); }
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override
    {
        result = 0;
        return index::ErrorCode::OK;
    }
    void Reset() override { mCurIdx = 0; }
    PostingIterator* Clone() const override
    {
        assert(false);
        return NULL;
    }

private:
    typedef std::vector<docpayload_t> DocPayloadVec;

    int mCurIdx;
    DocPayloadVec mDocPayloads;

private:
    IE_LOG_DECLARE();
};

class DocPayloadFilterProcessorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(DocPayloadFilterProcessorTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void check_data(std::string docpayloads, std::string boolvalues, DocPayloadFilterProcessor& docPayloadFilter)
    {
        FakeTruncatePostingIterator* fakeIt = new FakeTruncatePostingIterator();
        fakeIt->SetDocPayload(docpayloads);
        std::shared_ptr<PostingIterator> pIt(fakeIt);
        INDEXLIB_TEST_TRUE(docPayloadFilter.BeginFilter(index::DictKeyInfo(0), pIt));
        std::vector<int> boolVec;
        autil::StringUtil::fromString(boolvalues, boolVec, ";");
        for (size_t i = 0; i < boolVec.size(); ++i) {
            INDEXLIB_TEST_EQUAL(boolVec[i], docPayloadFilter.IsFiltered(i));
        }
    }
    void TestCaseForSimple()
    {
        DiversityConstrain diversityConstrain;
        diversityConstrain.SetFilterMaxValue(100);
        diversityConstrain.SetFilterMinValue(20);
        diversityConstrain.SetFilterMask(0xFF);
        DocPayloadFilterProcessor docPayloadFilter(diversityConstrain);
        check_data("10;39;101;279;20;100", "1;0;1;0;0;0", docPayloadFilter);
    }

    void TestCaseForMaskSmallerThanMinValue()
    {
        DiversityConstrain diversityConstrain;
        diversityConstrain.SetFilterMaxValue(100);
        diversityConstrain.SetFilterMinValue(20);
        diversityConstrain.SetFilterMask(0xF);
        DocPayloadFilterProcessor docPayloadFilter(diversityConstrain);
        check_data("10;39;101;279;20;100;0", "1;1;1;1;1;1;1", docPayloadFilter);
    }

    void TestCaseForMaskBetweenMinValueAndMaxValue()
    {
        DiversityConstrain diversityConstrain;
        diversityConstrain.SetFilterMaxValue(300);
        diversityConstrain.SetFilterMinValue(20);
        diversityConstrain.SetFilterMask(0xFF);
        DocPayloadFilterProcessor docPayloadFilter(diversityConstrain);
        check_data("10;20;300;3000;5431;301;0", "1;0;0;0;0;0;1", docPayloadFilter);
    }

    void TestCaseForMaskLargerThanMaxValue()
    {
        DiversityConstrain diversityConstrain;
        diversityConstrain.SetFilterMaxValue(300);
        diversityConstrain.SetFilterMinValue(20);
        diversityConstrain.SetFilterMask(0xFFF);
        DocPayloadFilterProcessor docPayloadFilter(diversityConstrain);
        check_data("10;20;300;3000;4266;301;0", "1;0;0;1;0;1;1", docPayloadFilter);
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, DocPayloadFilterProcessorTest);

INDEXLIB_UNIT_TEST_CASE(DocPayloadFilterProcessorTest, TestCaseForSimple);
INDEXLIB_UNIT_TEST_CASE(DocPayloadFilterProcessorTest, TestCaseForMaskSmallerThanMinValue);
INDEXLIB_UNIT_TEST_CASE(DocPayloadFilterProcessorTest, TestCaseForMaskBetweenMinValueAndMaxValue);
INDEXLIB_UNIT_TEST_CASE(DocPayloadFilterProcessorTest, TestCaseForMaskLargerThanMaxValue);
} // namespace indexlib::index::legacy
