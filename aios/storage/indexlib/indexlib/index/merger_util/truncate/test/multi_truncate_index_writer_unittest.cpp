#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/multi_truncate_index_writer.h"
#include "indexlib/index/merger_util/truncate/multi_truncate_writer_scheduler.h"
#include "indexlib/index/merger_util/truncate/simple_truncate_writer_scheduler.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer.h"
#include "indexlib/index/test/fake_index_reader.h"
#include "indexlib/test/unittest.h"

using namespace std;

namespace indexlib::index::legacy {

class FakeTruncateIndexWriter : public TruncateIndexWriter
{
public:
    FakeTruncateIndexWriter(df_t df)
    {
        mDf = df;
        mAddPostingCounter = 0;
    }

public:
    void Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos) override {}
    bool NeedTruncate(const TruncateTriggerInfo& info) const override
    {
        if (info.GetDF() >= mDf) {
            return true;
        }
        return false;
    }
    void AddPosting(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                    df_t docFreq = -1) override
    {
        mAddPostingCounter++;
    }

    void EndPosting() override {}

    uint32_t GetAddPostingCounter() { return mAddPostingCounter; }

    int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount, size_t outputSegmentCount) const override
    {
        return 0;
    }

private:
    df_t mDf;
    uint32_t mAddPostingCounter;
};
DEFINE_SHARED_PTR(FakeTruncateIndexWriter);

class MultiTruncateIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MultiTruncateIndexWriterTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForNeedTruncate()
    {
        CheckWriter(10, true, "5;2", "1;1");
        CheckWriter(2, true, "5;2", "0;1");
        CheckWriter(1, false, "5;2", "0;0");
    }

    void TestCaseForCreateTruncateWriteScheduler()
    {
        setenv("TRUNCATE_THREAD_COUNT", "10", 1);
        MultiTruncateIndexWriter multiWriter;
        TruncateWriterSchedulerPtr schedulerPtr = multiWriter.getTruncateWriterScheduler();
        MultiTruncateWriterSchedulerPtr multiSchedulerPtr =
            dynamic_pointer_cast<MultiTruncateWriterScheduler>(schedulerPtr);
        INDEXLIB_TEST_TRUE(multiSchedulerPtr != NULL);
        ASSERT_EQ((uint32_t)10, multiSchedulerPtr->GetThreadCount());

        multiWriter.SetTruncateThreadCount(4);
        schedulerPtr = multiWriter.getTruncateWriterScheduler();
        multiSchedulerPtr = dynamic_pointer_cast<MultiTruncateWriterScheduler>(schedulerPtr);
        INDEXLIB_TEST_TRUE(multiSchedulerPtr != NULL);
        ASSERT_EQ((uint32_t)4, multiSchedulerPtr->GetThreadCount());

        multiWriter.SetTruncateThreadCount(1);
        schedulerPtr = multiWriter.getTruncateWriterScheduler();
        ASSERT_TRUE(dynamic_pointer_cast<SimpleTruncateWriterScheduler>(schedulerPtr));
        unsetenv("TRUNCATE_THREAD_COUNT");
    }

private:
    // dfForIndexWriter separate by ';', example: "3;4;5"
    // expectAddPostingCount separate by ';', example: "1;2;1"
    void CheckWriter(df_t dfThreshold, bool expectNeedTruncate, const std::string& dfForIndexWriter,
                     const std::string& expectAddPostingCount)
    {
        vector<docid_t> dfsVec;
        autil::StringUtil::fromString(dfForIndexWriter, dfsVec, ";");
        vector<uint32_t> expectAddPostingCountVec;
        autil::StringUtil::fromString(expectAddPostingCount, expectAddPostingCountVec, ";");
        INDEXLIB_TEST_EQUAL(dfsVec.size(), expectAddPostingCountVec.size());

        std::vector<TruncateIndexWriterPtr> indexWriterVec;
        MultiTruncateIndexWriter multiWriter;
        for (size_t i = 0; i < dfsVec.size(); ++i) {
            TruncateIndexWriterPtr writerPtr(new FakeTruncateIndexWriter(dfsVec[i]));
            multiWriter.AddIndexWriter(writerPtr);
            indexWriterVec.push_back(writerPtr);
        }
        INDEXLIB_TEST_TRUE(multiWriter.NeedTruncate(TruncateTriggerInfo(index::DictKeyInfo(0), dfThreshold)) ==
                           expectNeedTruncate);

        int32_t postingCount = 0;
        for (size_t i = 0; i < expectAddPostingCountVec.size(); ++i) {
            postingCount += expectAddPostingCountVec[i];
        }
        INDEXLIB_TEST_EQUAL(postingCount, multiWriter.GetEstimatePostingCount());

        vector<docid_t> docIdVect(dfThreshold, 100);
        std::shared_ptr<PostingIterator> postingIter(new FakePostingIterator(docIdVect));
        multiWriter.AddPosting(index::DictKeyInfo(0), postingIter, postingIter->GetTermMeta()->GetDocFreq());

        for (size_t i = 0; i < expectAddPostingCountVec.size(); ++i) {
            FakeTruncateIndexWriterPtr fakeTruncateIndexWriter =
                dynamic_pointer_cast<FakeTruncateIndexWriter>(indexWriterVec[i]);
            INDEXLIB_TEST_EQUAL((uint32_t)expectAddPostingCountVec[i], fakeTruncateIndexWriter->GetAddPostingCounter());
        }
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, MultiTruncateIndexWriterTest);

INDEXLIB_UNIT_TEST_CASE(MultiTruncateIndexWriterTest, TestCaseForNeedTruncate);
INDEXLIB_UNIT_TEST_CASE(MultiTruncateIndexWriterTest, TestCaseForCreateTruncateWriteScheduler);
} // namespace indexlib::index::legacy
