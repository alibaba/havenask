#include "indexlib/index/normal/trie/test/trie_index_merger_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/normal/trie/trie_index_reader.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
using namespace autil;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, TrieIndexMergerTest);

TrieIndexMergerTest::TrieIndexMergerTest() {}

TrieIndexMergerTest::~TrieIndexMergerTest() {}

void TrieIndexMergerTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH(); }

void TrieIndexMergerTest::CaseTearDown() {}

void TrieIndexMergerTest::TestEstimateMemoryUse()
{
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.enablePackageFile = false;
    SingleFieldPartitionDataProvider provider(options);
    provider.Init(mRootDir, "string", SFP_TRIE);
    provider.Build("4567,dup,efg#45,4,456,dup,5", SFP_OFFLINE);

    TrieIndexMerger trieMerger;
    index_base::MergeItemHint hint;
    index_base::MergeTaskResourceVector taskResources;
    config::MergeIOConfig ioConfig;
    trieMerger.Init(provider.GetIndexConfig(), hint, taskResources, ioConfig);
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSIONID);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, true);
    PartitionDataPtr partitionData = provider.GetPartitionData();
    SegmentMergeInfos mergeInfos;
    mergeInfos.push_back(SegmentMergeInfo(0, *partitionData->GetSegmentData(0).GetSegmentInfo(), 1, 0));
    mergeInfos.push_back(SegmentMergeInfo(1, *partitionData->GetSegmentData(1).GetSegmentInfo(), 0, 3));

    ReclaimMapPtr reclaimMap(new ReclaimMap);
    index::MergerResource resource;
    resource.reclaimMap = ReclaimMapCreator::Create(false, mergeInfos, segDir->GetDeletionMapReader());

    trieMerger.BeginMerge(segDir);
    index_base::OutputSegmentMergeInfos outputSegmentMergeInfos;
    index_base::OutputSegmentMergeInfo outputSegMergeInfo;
    outputSegMergeInfo.path = mRootDir;
    outputSegMergeInfo.directory = DirectoryCreator::Create(mRootDir);
    outputSegMergeInfo.targetSegmentId = 100;
    outputSegmentMergeInfos.push_back(outputSegMergeInfo);
    int64_t memUse = trieMerger.EstimateMemoryUse(segDir, resource, mergeInfos, outputSegmentMergeInfos, false);
    trieMerger.Merge(resource, mergeInfos, outputSegmentMergeInfos);
    int64_t actualSize = (int64_t)trieMerger.mSimplePool.getPeakOfUsedBytes();
    float deltaPercent = (float)abs(actualSize - memUse) / (float)actualSize;
    ASSERT_TRUE(deltaPercent < 0.01);
}

void TrieIndexMergerTest::TestSimpleProcess()
{
    InnerTestMerge("1,2,12#delete:2#3,34,4#delete:3", "0,2", "1,2,12,3,34,4", "0,-1,1,-1,2,3");
    InnerTestMerge("1,2,3#3,2#delete:2#4,5", "0,1", "1,2,3,4,5", "2,-1,3,0,1");
}

void TrieIndexMergerTest::InnerTestMerge(const string& docStr, const string& mergeStr, const string& searchPks,
                                         const string& expectDocIds)
{
    tearDown();
    setUp();
    IndexPartitionOptions options;
    options.GetOfflineConfig().buildConfig.enablePackageFile = false;
    std::string mergeConfigStr = "{\"class_name\":\"default\",\"parameters\":{\"split_num\":\"2\"}}";
    autil::legacy::FromJsonString(options.GetMergeConfig().GetSplitSegmentConfig(), mergeConfigStr);

    SingleFieldPartitionDataProvider provider(options);
    provider.Init(mRootDir, "string", SFP_TRIE);
    provider.Build(docStr, SFP_OFFLINE);
    provider.Merge(mergeStr);

    vector<string> pks;
    StringUtil::fromString(searchPks, pks, ",");
    vector<docid_t> docids;
    StringUtil::fromString(expectDocIds, docids, ",");
    ASSERT_EQ(pks.size(), docids.size());

    TrieIndexReader reader;
    reader.Open(provider.GetIndexConfig(), provider.GetPartitionData());
    for (size_t i = 0; i < docids.size(); i++) {
        ASSERT_EQ(docids[i], reader.Lookup(pks[i]));
    }
}
}} // namespace indexlib::index
