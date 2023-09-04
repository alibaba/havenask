#include "indexlib/merger/test/demo_index_reducer.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/merger/test/demo_index_reduce_item.h"
#include "indexlib/merger/test/demo_indexer.h"
#include "indexlib/util/Exception.h"

using namespace std;

using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, DemoIndexReducer);

IndexReduceItemPtr DemoIndexReducer::CreateReduceItem()
{
    IndexReduceItemPtr reduceItem(new DemoIndexReduceItem());
    return reduceItem;
}

bool DemoIndexReducer::DoInit(const IndexerResourcePtr& resource)
{
    mMergedItemCounter = resource->GetStateCounter("DemoIndexReducer.mergedItemCount");
    mMergedItemCounter->Set(0);
    mMeta["plugin_path"] = resource->pluginPath;
    return true;
}

bool DemoIndexReducer::Reduce(const vector<IndexReduceItemPtr>& reduceItems,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                              bool isSortMerge, const ReduceResource& resource)
{
    assert(outputSegMergeInfos.size() == 1u);
    if (reduceItems.empty()) {
        IE_LOG(WARN, "reduceItems is empty, nothing to do");
        return false;
    }
    auto baseItem = DYNAMIC_POINTER_CAST(DemoIndexReduceItem, reduceItems[0]);
    int64_t totalItemCount = baseItem->mDataMap.size();
    for (size_t i = 1; i < reduceItems.size(); ++i) {
        auto item = DYNAMIC_POINTER_CAST(DemoIndexReduceItem, reduceItems[i]);
        for (auto iter = item->Begin(); iter != item->End(); ++iter) {
            if (baseItem->Add(iter->first, iter->second)) {
                totalItemCount++;
            }
        }
    }
    mMergedItemCounter->Set(totalItemCount);
    const auto& mergeDirectory = outputSegMergeInfos[0].directory;
    try {
        string fileJsonStr = autil::legacy::ToJsonString(baseItem->mDataMap);
        const FileWriterPtr& fileWriter = mergeDirectory->CreateFileWriter(baseItem->mFileName);
        fileWriter->Write(fileJsonStr.data(), fileJsonStr.size()).GetOrThrow();
        fileWriter->Close().GetOrThrow();
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "fail to store index file [%s], error: [%s]", baseItem->mFileName.c_str(), e.what());
        return false;
    }

    try {
        mMeta["segment_type"] = "merge_segment";
        if (resource.isEntireDataSet) {
            mMeta["is_complete_merge"] = "true";
        } else {
            mMeta["is_complete_merge"] = "false";
        }

        string fileJsonStr = autil::legacy::ToJsonString(mMeta);
        const FileWriterPtr& fileWriter = mergeDirectory->CreateFileWriter(DemoIndexer::META_FILE_NAME);
        fileWriter->Write(fileJsonStr.data(), fileJsonStr.size()).GetOrThrow();
        fileWriter->Close().GetOrThrow();
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "fail to store meta file [%s], error: [%s]", DemoIndexer::META_FILE_NAME.c_str(), e.what());
        return false;
    }
    return true;
}

int64_t
DemoIndexReducer::EstimateMemoryUse(const vector<DirectoryPtr>& indexDirs, const SegmentMergeInfos& segmentMergeInfos,
                                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                                    bool isSortMerge) const
{
    int64_t totalFileLength = 0;
    for (const auto& dir : indexDirs) {
        totalFileLength += dir->GetFileLength("demo_index_file");
    }
    return totalFileLength * 2;
}

vector<ReduceTask> DemoIndexReducer::CreateReduceTasks(const vector<DirectoryPtr>& indexDirs,
                                                       const SegmentMergeInfos& segmentInfos, uint32_t instanceCount,
                                                       bool isEntireDataSet, MergeTaskResourceManagerPtr& resMgr)
{
    vector<ReduceTask> reduceTasks;
    for (int32_t i = 0; i < 5; i++) {
        ReduceTask reduceTask;
        reduceTask.id = i;
        reduceTask.dataRatio = i * 0.1;
        reduceTask.resourceIds.push_back(i);
        if (i % 2 == 0) {
            reduceTask.resourceIds.push_back(i + 1);
        }
        reduceTasks.push_back(reduceTask);
    }
    return reduceTasks;
}

void DemoIndexReducer::EndParallelReduce(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                         int32_t totalParallelCount,
                                         const vector<MergeTaskResourceVector>& instResourceVec)
{
    for (const auto& info : outputSegMergeInfos) {
        DemoParallelReduceMeta meta(totalParallelCount);
        meta.Store(info.directory);
    }
}
}} // namespace indexlib::merger
