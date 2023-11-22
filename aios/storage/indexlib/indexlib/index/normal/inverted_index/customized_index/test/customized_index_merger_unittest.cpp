#include "indexlib/index/normal/inverted_index/customized_index/test/customized_index_merger_unittest.h"

#include "indexlib/config/customized_index_config.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/directory_creator.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CustomizedIndexMergerTest);

class FakeIndexReducer : public IndexReducer
{
public:
    FakeIndexReducer() : IndexReducer(util::KeyValueMap()) {}
    bool DoInit(const IndexerResourcePtr& resource) override { return true; }
    IndexReduceItemPtr CreateReduceItem() override { return IndexReduceItemPtr(); }
    bool Reduce(const std::vector<IndexReduceItemPtr>& reduceItems,
                const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                bool isSortMerge, const ReduceResource& resource) override
    {
        reduceInfos = outputSegMergeInfos;
        return true;
    }

    int64_t EstimateMemoryUse(const std::vector<file_system::DirectoryPtr>& indexDirs,
                              const index_base::SegmentMergeInfos& segmentInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                              bool isSortMerge) const override
    {
        return 0;
    }

    void EndParallelReduce(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
                           const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override
    {
        endReduceInfos = outputSegMergeInfos;
    }

    OutputSegmentMergeInfos reduceInfos;
    OutputSegmentMergeInfos endReduceInfos;
};

CustomizedIndexMergerTest::CustomizedIndexMergerTest() {}

CustomizedIndexMergerTest::~CustomizedIndexMergerTest() {}

void CustomizedIndexMergerTest::CaseSetUp() {}

void CustomizedIndexMergerTest::CaseTearDown() {}

void CustomizedIndexMergerTest::TestMultiOutputSegment()
{
    plugin::PluginManagerPtr pluginManager(new plugin::PluginManager(""));
    auto indexMerger = new CustomizedIndexMerger(pluginManager);
    auto fakeReducer = new FakeIndexReducer();
    indexMerger->mIndexReducer.reset(fakeReducer);
    MergeItemHint hint;
    hint.SetId(0);
    hint.SetTotalParallelCount(2);
    hint.SetTaskGroupId(0);
    indexMerger->mMergeHint = hint;
    config::IndexConfigPtr indexConfig(new config::CustomizedIndexConfig("cindex"));
    indexMerger->mIndexConfig = indexConfig;
    MergerResource resource;
    vector<set<docid_t>> dels = {{}};
    resource.reclaimMap = IndexTestUtil::CreateReclaimMap({100}, dels);
    resource.targetSegmentCount = 1;
    SegmentMergeInfos segMergeInfos;
    OutputSegmentMergeInfos outputSegMergeInfos;
    {
        OutputSegmentMergeInfo info;
        info.directory = GET_PARTITION_DIRECTORY()->MakeDirectory("part3/instance_0/segment_3_level_0/index/");
        outputSegMergeInfos.push_back(info);
    }

    indexMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);

    vector<MergeTaskResourceVector> instResourceVec;
    indexMerger->mMergeHint = MergeItemHint();

    outputSegMergeInfos.clear();
    {
        OutputSegmentMergeInfo info;
        info.directory = GET_PARTITION_DIRECTORY()->MakeDirectory("part3/segment_3_level_0/index/");
        outputSegMergeInfos.push_back(info);
    }

    indexMerger->EndParallelMerge(outputSegMergeInfos, 1, instResourceVec);

    ASSERT_EQ(1u, fakeReducer->reduceInfos.size());
    ASSERT_EQ(1u, fakeReducer->endReduceInfos.size());

    ASSERT_EQ("part3/instance_0/segment_3_level_0/index/cindex/inst_2_0",
              fakeReducer->reduceInfos[0].directory->GetLogicalPath());

    ASSERT_EQ("part3/segment_3_level_0/index/cindex", fakeReducer->endReduceInfos[0].directory->GetLogicalPath());

    delete indexMerger;
}

void CustomizedIndexMergerTest::TestParallelReduceMeta()
{
    ParallelReduceMeta meta(10);
    meta.Store(GET_PARTITION_DIRECTORY());
    meta.Store(GET_PARTITION_DIRECTORY());
    meta.Load(GET_PARTITION_DIRECTORY());
    ASSERT_EQ(10, meta.parallelCount);
}

}} // namespace indexlib::index
