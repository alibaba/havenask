#include "indexlib/common_define.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/test/fake_single_truncate_index_writer.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/unittest.h"

using namespace std;

using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib::index::legacy {

class FakeEvaluator : public Evaluator
{
public:
    FakeEvaluator() {};
    ~FakeEvaluator() {};

public:
    void Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) { return; }
};
DEFINE_SHARED_PTR(FakeEvaluator);

class FakeDocCollector : public DocCollector
{
public:
    FakeDocCollector() : DocCollector(0, 0, DocFilterProcessorPtr(), DocDistinctorPtr(), BucketVectorAllocatorPtr()) {}
    ~FakeDocCollector() {}

public:
    void ReInit() override {}
    void DoCollect(const index::DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt) override {}
    void Truncate() override { mMinValueDocId = 0; }

    bool Empty() { return true; }

    int64_t EstimateMemoryUse(uint32_t docCount) const override { return 0; }
};
DEFINE_SHARED_PTR(FakeDocCollector);

class FakeTruncateTrigger : public TruncateTrigger
{
public:
    FakeTruncateTrigger() {};
    ~FakeTruncateTrigger() {};

public:
    bool NeedTruncate(const TruncateTriggerInfo& info) { return true; };
};
DEFINE_SHARED_PTR(FakeTruncateTrigger);

class SingleTruncateIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SingleTruncateIndexWriterTest);
    void CaseSetUp() override { mRootPath = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

    void TestCaseForHasTruncated()
    {
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        IndexConfigPtr indexConfig(new PackageIndexConfig("name", it_string));
        FakeSingleTruncateIndexWriter truncateIndexWriter(indexConfig, reclaimMap);
        truncateIndexWriter.InsertTruncateDictKey(index::DictKeyInfo(1));
        truncateIndexWriter.InsertTruncateDictKey(index::DictKeyInfo(2));
        truncateIndexWriter.InsertTruncateDictKey(index::DictKeyInfo::NULL_TERM);
        INDEXLIB_TEST_TRUE(truncateIndexWriter.HasTruncated(index::DictKeyInfo(1)));
        INDEXLIB_TEST_TRUE(truncateIndexWriter.HasTruncated(index::DictKeyInfo(2)));
        INDEXLIB_TEST_TRUE(truncateIndexWriter.HasTruncated(index::DictKeyInfo::NULL_TERM));
        ASSERT_FALSE(truncateIndexWriter.HasTruncated(index::DictKeyInfo(3)));
    }

    void TestCaseForCreatePostingWriter()
    {
        // create truncate index writer
        IndexConfigPtr indexConfig(new PackageIndexConfig("name", it_string));
        index_base::OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.path = mRootPath + "/test";
        outputSegmentMergeInfo.directory = test::DirectoryCreator::Create(outputSegmentMergeInfo.path);
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        outputSegmentMergeInfos.push_back(outputSegmentMergeInfo);
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        FakeSingleTruncateIndexWriter truncateIndexWriter(indexConfig, reclaimMap);
        config::MergeIOConfig ioConfig;
        EvaluatorPtr evalutor(new FakeEvaluator());
        DocCollectorPtr docCollector(new FakeDocCollector());
        TruncateTriggerPtr truncateTrigger(new FakeTruncateTrigger());
        DocInfoAllocatorPtr docInfoAllocator(new DocInfoAllocator());
        truncateIndexWriter.Init(evalutor, docCollector, truncateTrigger, "", docInfoAllocator, ioConfig);

        // test CreatePostingWriter for different index types
        // NOT test change
        std::shared_ptr<legacy::MultiSegmentPostingWriter> writer = truncateIndexWriter.CreatePostingWriter(it_expack);
        INDEXLIB_TEST_TRUE(writer.get() != NULL);
        writer = truncateIndexWriter.CreatePostingWriter(it_text);
        INDEXLIB_TEST_TRUE(writer.get() != NULL);

        // PostingWriterImplPtr expackWriterPtr =
        //     dynamic_pointer_cast<PostingWriterImpl>(writer);
        // PostingWriterImplPtr packWriterPtr =
        //     dynamic_pointer_cast<PostingWriterImpl>(writer);
        // INDEXLIB_TEST_TRUE(packWriterPtr != NULL);
        writer = truncateIndexWriter.CreatePostingWriter(it_primarykey64);
        INDEXLIB_TEST_TRUE(writer.get() == NULL);
        writer = truncateIndexWriter.CreatePostingWriter(it_primarykey128);
        INDEXLIB_TEST_TRUE(writer.get() == NULL);
        writer = truncateIndexWriter.CreatePostingWriter(it_trie);
        INDEXLIB_TEST_TRUE(writer.get() == NULL);
    }

    void TestCaseForGenerateMetaValue()
    {
        IndexConfigPtr indexConfig(new PackageIndexConfig("name", it_string));
        index_base::OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.path = mRootPath + "/test";
        outputSegmentMergeInfo.directory = test::DirectoryCreator::Create(outputSegmentMergeInfo.path);
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        outputSegmentMergeInfos.push_back(outputSegmentMergeInfo);
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        FakeSingleTruncateIndexWriter truncateIndexWriter(indexConfig, reclaimMap);
        config::MergeIOConfig ioConfig;
        EvaluatorPtr evaluator(new FakeEvaluator());
        DocCollectorPtr docCollector(new FakeDocCollector());
        TruncateTriggerPtr truncateTrigger(new FakeTruncateTrigger());
        DocInfoAllocatorPtr docInfoAllocator(new DocInfoAllocator());
        truncateIndexWriter.Init(evaluator, docCollector, truncateTrigger, "", docInfoAllocator, ioConfig);

        string metaValue;
        truncateIndexWriter.SetLastDocValue("123");
        truncateIndexWriter.GenerateMetaValue(std::shared_ptr<PostingIterator>(), index::DictKeyInfo(2), metaValue);
        INDEXLIB_TEST_TRUE(metaValue == "2\t123\n");

        truncateIndexWriter.GenerateMetaValue(std::shared_ptr<PostingIterator>(), index::DictKeyInfo::NULL_TERM,
                                              metaValue);
        INDEXLIB_TEST_TRUE(metaValue == "18446744073709551615:true\t123\n") << metaValue;
    }

private:
    string mRootPath;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SingleTruncateIndexWriterTest);

INDEXLIB_UNIT_TEST_CASE(SingleTruncateIndexWriterTest, TestCaseForHasTruncated);
INDEXLIB_UNIT_TEST_CASE(SingleTruncateIndexWriterTest, TestCaseForCreatePostingWriter);
INDEXLIB_UNIT_TEST_CASE(SingleTruncateIndexWriterTest, TestCaseForGenerateMetaValue);
} // namespace indexlib::index::legacy
