 #include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/test/fake_single_truncate_index_writer.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info_allocator.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
using namespace std::tr1;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);


IE_NAMESPACE_BEGIN(index);

class FakeEvaluator : public Evaluator
{
public:
    FakeEvaluator() {};
    ~FakeEvaluator() {};

public:
    void Evaluate(docid_t docId, 
                  const PostingIteratorPtr& postingIter,
                  DocInfo *docInfo)
    {
        return;
    }
};
DEFINE_SHARED_PTR(FakeEvaluator);

class FakeDocCollector : public DocCollector
{
public:
    FakeDocCollector() 
        : DocCollector(0, 0, DocFilterProcessorPtr(),
                       DocDistinctorPtr(), BucketVectorAllocatorPtr())
    {}
    ~FakeDocCollector() {}

public:
    void ReInit() override {}
    void DoCollect(dictkey_t key, const PostingIteratorPtr& postingIt) override {}
    void Truncate() override { mMinValueDocId = 0; }

    bool Empty() { return true; }

    int64_t EstimateMemoryUse(uint32_t docCount) const override
    {
        return 0;
    }
};
DEFINE_SHARED_PTR(FakeDocCollector);

class FakeTruncateTrigger : public TruncateTrigger
{
public:
    FakeTruncateTrigger() {};
    ~FakeTruncateTrigger() {};

public:
    bool NeedTruncate(const TruncateTriggerInfo &info) { return true; };
};
DEFINE_SHARED_PTR(FakeTruncateTrigger);

class SingleTruncateIndexWriterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SingleTruncateIndexWriterTest);
    void CaseSetUp() override
    {
        mRootPath = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForHasTruncated()
    {
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        IndexConfigPtr indexConfig(new PackageIndexConfig("name", it_string));
        FakeSingleTruncateIndexWriter truncateIndexWriter(indexConfig, reclaimMap);
        truncateIndexWriter.InsertTruncateDictKey(1);
        truncateIndexWriter.InsertTruncateDictKey(2);
        INDEXLIB_TEST_TRUE(truncateIndexWriter.HasTruncated(1));
        INDEXLIB_TEST_TRUE(truncateIndexWriter.HasTruncated(2));
        ASSERT_FALSE(truncateIndexWriter.HasTruncated(3));
    }

    void TestCaseForCreatePostingWriter()
    {
        // create truncate index writer
        IndexConfigPtr indexConfig(new PackageIndexConfig("name", it_string));
        index_base::OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.path = mRootPath + "/test";
        outputSegmentMergeInfo.directory = DirectoryCreator::Create(outputSegmentMergeInfo.path);
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        outputSegmentMergeInfos.push_back(outputSegmentMergeInfo);
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        FakeSingleTruncateIndexWriter truncateIndexWriter(indexConfig, reclaimMap);
        config::MergeIOConfig ioConfig;
        EvaluatorPtr evalutor(new FakeEvaluator());
        DocCollectorPtr docCollector(new FakeDocCollector());
        TruncateTriggerPtr truncateTrigger(new FakeTruncateTrigger());
        DocInfoAllocatorPtr docInfoAllocator(new DocInfoAllocator());
        truncateIndexWriter.Init(evalutor, docCollector, truncateTrigger,
                "", docInfoAllocator, ioConfig);

        // test CreatePostingWriter for different index types
        // NOT test change 
        MultiSegmentPostingWriterPtr writer = truncateIndexWriter.CreatePostingWriter(it_expack);
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
        outputSegmentMergeInfo.directory = DirectoryCreator::Create(outputSegmentMergeInfo.path);
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        outputSegmentMergeInfos.push_back(outputSegmentMergeInfo);
        ReclaimMapPtr reclaimMap(new ReclaimMap);
        FakeSingleTruncateIndexWriter truncateIndexWriter(indexConfig, reclaimMap);
        config::MergeIOConfig ioConfig;
        EvaluatorPtr evaluator(new FakeEvaluator());
        DocCollectorPtr docCollector(new FakeDocCollector());
        TruncateTriggerPtr truncateTrigger(new FakeTruncateTrigger());
        DocInfoAllocatorPtr docInfoAllocator(new DocInfoAllocator());
        truncateIndexWriter.Init(evaluator, docCollector, truncateTrigger,
                "", docInfoAllocator, ioConfig);

        string metaValue;
        truncateIndexWriter.SetLastDocValue("123");
        truncateIndexWriter.GenerateMetaValue(PostingIteratorPtr(), 2, metaValue);
        INDEXLIB_TEST_TRUE(metaValue == "2\t123\n");
    }

    void TestCaseForSaveDictKey()
    {
        IndexConfigPtr indexConfig(new PackageIndexConfig("name", it_string));
        index_base::OutputSegmentMergeInfo outputSegmentMergeInfo;
        outputSegmentMergeInfo.path = mRootPath + "/test";
        outputSegmentMergeInfo.directory = DirectoryCreator::Create(outputSegmentMergeInfo.path);
        index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
        outputSegmentMergeInfos.push_back(outputSegmentMergeInfo);

        ReclaimMapPtr reclaimMap(new ReclaimMap);
        FakeSingleTruncateIndexWriter truncateIndexWriter(indexConfig, reclaimMap);
        config::MergeIOConfig ioConfig;
        EvaluatorPtr evaluator(new FakeEvaluator());
        DocCollectorPtr docCollector(new FakeDocCollector());
        TruncateTriggerPtr truncateTrigger(new FakeTruncateTrigger());
        DocInfoAllocatorPtr docInfoAllocator(new DocInfoAllocator());
        truncateIndexWriter.Init(evaluator, docCollector, truncateTrigger,
                "", docInfoAllocator, ioConfig);
        std::string metaValue;
        truncateIndexWriter.SetDictValue((dictvalue_t)1);
        truncateIndexWriter.SaveDictKey(3, 10);
        
        //not record anymore
        // SingleTruncateIndexWriter::DictionaryRecords records;
        // records.push_back(make_pair((dictkey_t)3, (dictvalue_t)1));
        // INDEXLIB_TEST_TRUE(truncateIndexWriter.CheckmDictionaryRecords(records));
    }

private:
    string mRootPath;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SingleTruncateIndexWriterTest);

INDEXLIB_UNIT_TEST_CASE(SingleTruncateIndexWriterTest, TestCaseForHasTruncated);
INDEXLIB_UNIT_TEST_CASE(SingleTruncateIndexWriterTest, TestCaseForCreatePostingWriter);
INDEXLIB_UNIT_TEST_CASE(SingleTruncateIndexWriterTest, TestCaseForGenerateMetaValue);
INDEXLIB_UNIT_TEST_CASE(SingleTruncateIndexWriterTest, TestCaseForSaveDictKey);

IE_NAMESPACE_END(index);
