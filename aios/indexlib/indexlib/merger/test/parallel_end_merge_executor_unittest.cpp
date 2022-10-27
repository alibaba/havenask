#include <autil/StringUtil.h>
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/merger/test/parallel_end_merge_executor_unittest.h"
#include "indexlib/merger/test/merge_work_item_creator_mock.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/file_system/directory.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, ParallelEndMergeExecutorTest);

class NewFakeIndexMerger : public FakeIndexMerger
{
public:
    NewFakeIndexMerger() {}
    DECLARE_INDEX_MERGER_IDENTIFIER(NewFakeIndexMerger);

    void EndParallelMerge(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        int32_t totalParallelCount,
        const std::vector<index_base::MergeTaskResourceVector>& instResourceVec) override
    {
        assert(outputSegMergeInfos.size() == 1);
        auto dir = outputSegMergeInfos[0].directory->GetPath();
        string content;
        content = StringUtil::toString(totalParallelCount) + "#";
        for (size_t i = 0; i < instResourceVec.size(); i++)
        {
            if (instResourceVec[i].empty())
            {
                continue;
            }
            content += StringUtil::toString(i) + ":" +
                       StringUtil::toString(instResourceVec[i].size()) + ";";
        }
        FileSystemWrapper::AtomicStore(dir + "/test_end_parallel_merge", content);
    }
};
    
class MockParallelEndMergeExecutor : public ParallelEndMergeExecutor
{
public:
    MockParallelEndMergeExecutor(
            const config::IndexPartitionSchemaPtr &schema,
            const plugin::PluginManagerPtr& pluginManager,
            const file_system::DirectoryPtr& rootDirectory)
        : ParallelEndMergeExecutor(schema, pluginManager, rootDirectory)
    {
    }
    
public:
    index::IndexMergerPtr CreateIndexMerger(
        const MergeTaskItem& item, const config::IndexPartitionSchemaPtr& schema) const override
    {
        return index::IndexMergerPtr(new NewFakeIndexMerger());
    }

    index::AttributeMergerPtr CreateAttributeMerger(
            const MergeTaskItem &item, 
            const config::IndexPartitionSchemaPtr &schema) const override
    {
        return index::AttributeMergerPtr(new FakeAttributeMerger());
    }
    
    index::SummaryMergerPtr CreateSummaryMerger(
            const MergeTaskItem &item, 
            const config::IndexPartitionSchemaPtr &schema) const override
    {
        return index::SummaryMergerPtr(new FakeSummaryMerger());
    }
};

ParallelEndMergeExecutorTest::ParallelEndMergeExecutorTest()
{
}

ParallelEndMergeExecutorTest::~ParallelEndMergeExecutorTest()
{
}

void ParallelEndMergeExecutorTest::CaseSetUp()
{
}

void ParallelEndMergeExecutorTest::CaseTearDown()
{
}

void ParallelEndMergeExecutorTest::TestExtractParallelMergeTaskGroup()
{
    vector<string> descStr = {
        "0,0,2;1,1,3;1,0,3;1,2,3;0,1,2;3,0,1",
        "4,0,2;4,1,2;5,0,1;-1,0,2"
    };
    vector<MergeTaskItems> taskItemVec = MakeMergeTaskItemsVec(descStr);
    IndexMergeMetaPtr mergeMeta(new IndexMergeMeta);
    mergeMeta->SetMergeTaskItems(taskItemVec);

    vector<MergeTaskItems> taskGroups =
        ParallelEndMergeExecutor::ExtractParallelMergeTaskGroup(mergeMeta);
    CheckMergeTaskGroups(taskGroups, "0,2;1,3;4,2");

    {
        // test taskGroup.size() != totalParallelCount
        vector<string> descStr = {
            "0,0,2;1,1,3;1,0,3;0,1,2;3,0,1",
            "4,0,2;4,1,2;5,0,1;-1,0,2"
        };
        taskItemVec = MakeMergeTaskItemsVec(descStr);
        mergeMeta->SetMergeTaskItems(taskItemVec);
        ASSERT_ANY_THROW(ParallelEndMergeExecutor::ExtractParallelMergeTaskGroup(mergeMeta));
    }

    {
        // test same taskGroupId with different planIdx
        vector<string> descStr = {
            "0,0,2;1,1,3;1,0,3;0,1,2;3,0,1",
            "1,2,3;4,0,2;4,1,2;5,0,1;-1,0,2"
        };
        taskItemVec = MakeMergeTaskItemsVec(descStr);
        mergeMeta->SetMergeTaskItems(taskItemVec);
        ASSERT_ANY_THROW(ParallelEndMergeExecutor::ExtractParallelMergeTaskGroup(mergeMeta));
    }
}

void ParallelEndMergeExecutorTest::TestSingleGroupParallelEndMerge()
{
    MockParallelEndMergeExecutor executor(
            config::IndexPartitionSchemaPtr(),
            plugin::PluginManagerPtr(), GET_PARTITION_DIRECTORY());

    vector<string> descStr = {"1,1,3;1,0,3;1,2,3"};
    vector<MergeTaskItems> taskItemVec = MakeMergeTaskItemsVec(descStr);
    MergeTaskItems taskGroup = taskItemVec[0];

    MergeTaskResourceManagerPtr mgr(new MergeTaskResourceManager);
    mgr->Init(GET_TEST_DATA_PATH());
    for (size_t i = 0; i < 3; i++)
    {
        mgr->DeclareResource("abc", 3, StringUtil::toString(i));
    }
    taskGroup[0].mMergeType = INDEX_TASK_NAME;
    taskGroup[0].mParallelMergeItem.AddResource(0);
    taskGroup[2].mParallelMergeItem.AddResource(1);
    taskGroup[2].mParallelMergeItem.AddResource(2);

    IndexMergeMetaPtr mergeMeta(new IndexMergeMeta);
    mergeMeta->SetMergeTaskItems(taskItemVec);
    mergeMeta->SetMergeTaskResourceVec(mgr->GetResourceVec());

    MergePlan plan;
    plan.SetTargetSegmentId(0, 0);
    Version version;
    version.AddSegment(0);
    mergeMeta->AddMergePlan(plan);
    mergeMeta->SetTargetVersion(version);
    vector<set<docid_t>> delDocIdSets;
    delDocIdSets.push_back({});
    auto reclaimMap = index::IndexTestUtil::CreateReclaimMap({ 100 }, delDocIdSets, false);
    mergeMeta->AddMergePlanResource(reclaimMap, index::ReclaimMapPtr(), index::BucketMaps());

    FileSystemWrapper::MkDir(GET_TEST_DATA_PATH() + "/segment_0_level_0/index/", true);

    executor.SingleGroupParallelEndMerge(mergeMeta, taskGroup);

    string metaPath = GET_TEST_DATA_PATH() + "/segment_0_level_0/index/test_end_parallel_merge";
    ASSERT_TRUE(FileSystemWrapper::IsExist(metaPath));

    string content;
    FileSystemWrapper::AtomicLoad(metaPath, content);
    ASSERT_EQ(string("3#0:1;2:2;"), content);
}

vector<MergeTaskItems> ParallelEndMergeExecutorTest::MakeMergeTaskItemsVec(
        const vector<string>& descStrVec)
{
    vector<MergeTaskItems> ret;
    for (size_t idx = 0; idx < descStrVec.size(); idx++)
    {
        MergeTaskItems items; 
        vector<vector<int32_t>> infos;
        StringUtil::fromString(descStrVec[idx], infos, ",", ";");
        for (size_t i = 0; i < infos.size(); i++)
        {
            assert(infos[i].size() == 3);
            MergeTaskItem item;
            item.mMergePlanIdx = (uint32_t)idx;
            item.mName = StringUtil::toString(infos[i][0]);

            ParallelMergeItem parallelItem;
            parallelItem.SetTaskGroupId(infos[i][0]);
            parallelItem.SetId(infos[i][1]);
            parallelItem.SetTotalParallelCount(infos[i][2]);
            item.SetParallelMergeItem(parallelItem);

            items.push_back(item);
        }
        ret.push_back(items);
    }
    return ret;
}

void ParallelEndMergeExecutorTest::CheckMergeTaskGroups(
        const vector<MergeTaskItems>& taskGroups, const string& groupInfo)
{
    vector<vector<int32_t>> infos;
    StringUtil::fromString(groupInfo, infos, ",", ";");
    ASSERT_EQ(taskGroups.size(), infos.size());

    for(size_t i = 0; i < infos.size(); i++)
    {
        ASSERT_EQ(taskGroups[i].size(), (size_t)infos[i][1]);
        ASSERT_EQ(taskGroups[i][0].GetParallelMergeItem().GetTaskGroupId(), infos[i][0]);
    }
}
 
IE_NAMESPACE_END(merger);

