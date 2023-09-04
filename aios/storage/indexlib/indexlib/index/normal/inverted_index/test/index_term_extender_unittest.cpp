#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingDumper.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"
#include "indexlib/index/normal/inverted_index/test/index_term_extender_mock.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index { namespace legacy {

class PostingMergerMock : public PostingMerger
{
public:
    PostingMergerMock(df_t docFreq, ttf_t totalTF) : mTotalTF(totalTF), mDocFreq(docFreq) {}
    ~PostingMergerMock() {}

public:
    void Merge(const index::SegmentTermInfos& segTermInfos, const index::ReclaimMapPtr& reclaimMap) override {}

    void SortByWeightMerge(const index::SegmentTermInfos& segTermInfos, const index::ReclaimMapPtr& reclaimMap) override
    {
    }

    void Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources) override {}
    uint64_t GetDumpLength() const { return 0; }
    ttf_t GetTotalTF() override { return mTotalTF; }
    df_t GetDocFreq() override { return mDocFreq; }
    uint64_t GetPostingLength() const override { return 0; }
    bool IsCompressedPostingHeader() const override { return true; }
    std::shared_ptr<PostingIterator> CreatePostingIterator() override { return std::shared_ptr<PostingIterator>(); }

private:
    ttf_t mTotalTF;
    df_t mDocFreq;
};

class IndexTermExtenderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(IndexTermExtenderTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestCaseForCreatePostingIterator()
    {
        {
            // normal case
            IndexConfigPtr indexConfig(new PackageIndexConfig("test", it_pack));
            IndexTermExtenderMock termExtender(indexConfig);
            std::shared_ptr<PostingIterator> postingIter;
            PostingMergerPtr postingMerger(new PostingMergerMock(1, 2));
            postingIter =
                termExtender.CreateCommonPostingIterator(util::ByteSliceListPtr(), 0, SegmentTermInfo::TM_BITMAP);
            BufferedPostingIterator* bufferedPostingIter;
            bufferedPostingIter = dynamic_cast<BufferedPostingIterator*>(postingIter.get());
            INDEXLIB_TEST_TRUE(bufferedPostingIter != NULL);
        }
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, IndexTermExtenderTest);

INDEXLIB_UNIT_TEST_CASE(IndexTermExtenderTest, TestCaseForCreatePostingIterator);
}}} // namespace indexlib::index::legacy
