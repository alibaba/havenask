#include "indexlib/index/inverted_index/truncate/MultiTruncateIndexWriter.h"

#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/index/inverted_index/truncate/AttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/DocInfoAllocator.h"
#include "indexlib/index/inverted_index/truncate/MultiTruncateWriterScheduler.h"
#include "indexlib/index/inverted_index/truncate/NoSortTruncateCollector.h"
#include "indexlib/index/inverted_index/truncate/SimpleTruncateWriterScheduler.h"
#include "indexlib/index/inverted_index/truncate/SingleTruncateIndexWriter.h"
#include "indexlib/index/inverted_index/truncate/test/FakePostingIterator.h"
#include "indexlib/index/inverted_index/truncate/test/MockHelper.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class MultiTruncateIndexWriterTest : public TESTBASE
{
public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(MultiTruncateIndexWriterTest, TestSetTruncateThreadCount)
{
    auto writer = std::make_unique<MultiTruncateIndexWriter>();
    writer->SetTruncateThreadCount(/*threadCount*/ 1);
    auto singleScheduler = dynamic_cast<SimpleTruncateWriterScheduler*>(writer->_scheduler.get());
    ASSERT_TRUE(singleScheduler != nullptr);

    writer->SetTruncateThreadCount(/*threadCount*/ 0);
    auto multiScheduler = dynamic_cast<MultiTruncateWriterScheduler*>(writer->_scheduler.get());
    ASSERT_TRUE(singleScheduler != nullptr);

    writer->SetTruncateThreadCount(/*threadCount*/ 513);
    multiScheduler = dynamic_cast<MultiTruncateWriterScheduler*>(writer->_scheduler.get());
    ASSERT_TRUE(singleScheduler != nullptr);

    writer->SetTruncateThreadCount(/*threadCount*/ 512);
    multiScheduler = dynamic_cast<MultiTruncateWriterScheduler*>(writer->_scheduler.get());
    ASSERT_TRUE(multiScheduler != nullptr);
}

TEST_F(MultiTruncateIndexWriterTest, TestEmptyWriters)
{
    auto writer = std::make_unique<MultiTruncateIndexWriter>();
    writer->Init({});

    auto postingIter = std::make_shared<MockPostingIterator>();
    DictKeyInfo dictKey;
    auto status = writer->AddPosting(dictKey, postingIter, /*docFreq*/ -1);
    ASSERT_TRUE(status.IsOK());

    TruncateTriggerInfo info;
    ASSERT_FALSE(writer->NeedTruncate(info));

    writer->EndPosting();
}

TEST_F(MultiTruncateIndexWriterTest, TestNeedTruncate)
{
    auto mockWriter1 = std::make_shared<MockTruncateIndexWriter>();
    auto mockWriter2 = std::make_shared<MockTruncateIndexWriter>();
    auto multiWriter = std::make_unique<MultiTruncateIndexWriter>();
    multiWriter->AddIndexWriter(mockWriter1);
    multiWriter->AddIndexWriter(mockWriter2);

    EXPECT_CALL(*mockWriter1, NeedTruncate(_)).WillRepeatedly(Return(false));
    EXPECT_CALL(*mockWriter2, NeedTruncate(_)).WillRepeatedly(Return(true));

    TruncateTriggerInfo info;
    ASSERT_TRUE(multiWriter->NeedTruncate(info));
    ASSERT_EQ(1, multiWriter->_estimatePostingCount);
}

TEST_F(MultiTruncateIndexWriterTest, TestValidWriters)
{
    auto mockScheduler = std::make_unique<MockTruncateWriterScheduler>();
    auto mockWriter1 = std::make_shared<MockTruncateIndexWriter>();
    auto mockWriter2 = std::make_shared<MockTruncateIndexWriter>();
    auto multiWriter = std::make_unique<MultiTruncateIndexWriter>();
    multiWriter->AddIndexWriter(mockWriter1);
    multiWriter->AddIndexWriter(mockWriter2);
    ASSERT_EQ(2, multiWriter->_truncateIndexWriters.size());

    // ::testing::InSequence dummy;
    EXPECT_CALL(*mockWriter1, Init(_)).WillRepeatedly(Return());
    EXPECT_CALL(*mockWriter2, Init(_)).WillRepeatedly(Return());
    multiWriter->Init({});

    auto mockSchedulerRawPtr = mockScheduler.get();
    multiWriter->_scheduler = std::move(mockScheduler);

    auto ok = Status::OK();
    auto failed1 = Status::InternalError();
    EXPECT_CALL(*mockSchedulerRawPtr, WaitFinished())
        .WillOnce(Return(failed1))
        .WillOnce(Return(ok))
        .WillOnce(Return(ok));
    EXPECT_CALL(*mockWriter1, NeedTruncate(_)).WillRepeatedly(Return(false));
    EXPECT_CALL(*mockWriter2, NeedTruncate(_)).WillRepeatedly(Return(false));
    auto postingIter = std::make_shared<MockPostingIterator>();
    DictKeyInfo dictKey;
    // wait finish failed
    auto status = multiWriter->AddPosting(dictKey, postingIter, /*docFreq*/ -1);
    ASSERT_TRUE(status.IsInternalError());
    // wait finish success
    status = multiWriter->AddPosting(dictKey, postingIter, /*docFreq*/ -1);
    ASSERT_TRUE(status.IsOK());

    EXPECT_CALL(*mockWriter1, EndPosting()).WillRepeatedly(Return());
    EXPECT_CALL(*mockWriter2, EndPosting()).WillRepeatedly(Return());
    multiWriter->EndPosting();

    ASSERT_EQ(0, multiWriter->_truncateIndexWriters.size());
    // AddPosting中new workItem, 如果mock scheduler, 无法delete, 导致报错, 因此暂不测那块
    // ::testing::Mock::AllowLeak(mockWriter2.get());
    // EXPECT_TRUE(::testing::Mock::VerifyAndClearExpectations(mockWriter1.get()));
}

} // namespace indexlib::index
