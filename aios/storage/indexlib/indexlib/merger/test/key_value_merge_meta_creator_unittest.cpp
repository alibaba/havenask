#include "indexlib/merger/test/key_value_merge_meta_creator_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/test/merge_task_maker.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, KeyValueMergeMetaCreatorTest);

KeyValueMergeMetaCreatorTest::KeyValueMergeMetaCreatorTest() {}

KeyValueMergeMetaCreatorTest::~KeyValueMergeMetaCreatorTest() {}

void KeyValueMergeMetaCreatorTest::CaseSetUp()
{
    mSchema = SchemaMaker::MakeKVSchema("key:int32;value:uint64;", "key", "value");
}

void KeyValueMergeMetaCreatorTest::CaseTearDown() {}

void KeyValueMergeMetaCreatorTest::TestSimpleProcess()
{
    string rootDir = GET_TEMP_DATA_PATH();
    {
        // make two segment in rootDir
        PartitionStateMachine psm;
        IndexPartitionOptions options;
        ASSERT_TRUE(psm.Init(mSchema, options, rootDir));
        ASSERT_TRUE(psm.Transfer(PE_BUILD_FULL, "cmd=add,key=1,value=1,ts=101;", "", ""));
        ASSERT_TRUE(psm.Transfer(PE_BUILD_INC, "cmd=add,key=2,value=2,ts=102;", "", ""));
    }
    GET_FILE_SYSTEM()->TEST_MountLastVersion();
    Version version;
    VersionLoader::GetVersion(GET_PARTITION_DIRECTORY(), version, INVALID_VERSION);
    merger::SegmentDirectoryPtr segDir(new merger::SegmentDirectory(GET_PARTITION_DIRECTORY(), version));
    segDir->Init(false, false);
    KeyValueMergeMetaCreator mergeMetaCreator(mSchema);

    MergeConfig mergeConfig;
    DumpStrategyPtr dumpStrategy(new DumpStrategy(segDir));
    auto segMergeInfos = MergeMetaCreator::CreateSegmentMergeInfos(mSchema, mergeConfig, segDir);
    mergeMetaCreator.Init(segDir, segMergeInfos, mergeConfig, dumpStrategy, plugin::PluginManagerPtr(), 1);

    MergeTask task;
    MergeTaskMaker::CreateMergeTask("0,1;", task, segMergeInfos);
    IndexMergeMeta* mergeMeta = mergeMetaCreator.Create(task);
    CheckMergeMeta(*mergeMeta, "2:2", "0", "2");
    delete mergeMeta;
}

// version : "version_id : segment_id[, segment_id]"
// mergeTaskItems : "merge_plan_idx[, merge_plan_idx][;merge_plan_idx[, merge_plan_idx]]"
// mergePlanMetas : "target segment id[, tgarget segment id]"
void KeyValueMergeMetaCreatorTest::CheckMergeMeta(IndexMergeMeta& mergeMeta, const string& version,
                                                  const string& mergeTaskItems, const string& mergePlanMetas)
{
    // check target version
    vector<string> versionInfo;
    StringUtil::fromString(version, versionInfo, ":");
    assert(versionInfo.size() == 2);
    versionid_t versionId = StringUtil::fromString<versionid_t>(versionInfo[0]);
    vector<segmentid_t> segmentIds;
    StringUtil::fromString(versionInfo[1], segmentIds, ",");
    ASSERT_EQ(versionId, mergeMeta.GetTargetVersion().GetVersionId());
    ASSERT_EQ(segmentIds, mergeMeta.GetTargetVersion().GetSegmentVector());

    // check merge task item
    vector<vector<uint32_t>> taskItems;
    StringUtil::fromString(mergeTaskItems, taskItems, ";", ",");
    const vector<MergeTaskItems>& actualMergeTaskItems = mergeMeta.GetMergeTaskItemsVec();
    ASSERT_EQ(taskItems.size(), actualMergeTaskItems.size());
    for (size_t i = 0; i < taskItems.size(); ++i) {
        ASSERT_EQ(taskItems[i].size(), actualMergeTaskItems[i].size());
        for (size_t j = 0; j < taskItems.size(); ++j) {
            ASSERT_EQ(taskItems[i][j], actualMergeTaskItems[i][j].mMergePlanIdx);
        }
    }

    // check merge plan
    vector<segmentid_t> targetSegIds;
    StringUtil::fromString(mergePlanMetas, targetSegIds, ",");

    ASSERT_EQ(targetSegIds.size(), mergeMeta.GetMergePlanCount());
    for (size_t i = 0; i < targetSegIds.size(); ++i) {
        ASSERT_EQ(targetSegIds[i], mergeMeta.GetMergePlan(i).GetTargetSegmentId(0));
    }
}
}} // namespace indexlib::merger
