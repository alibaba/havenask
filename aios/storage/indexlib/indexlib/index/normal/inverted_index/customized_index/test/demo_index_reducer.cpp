#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_reducer.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_reduce_item.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_indexer.h"
#include "indexlib/util/Exception.h"

using namespace std;

using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DemoIndexReducer);

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

bool DemoIndexReducer::Reduce(const std::vector<IndexReduceItemPtr>& reduceItems,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                              bool isSortMerge, const ReduceResource& resource)
{
    const auto& mergeDirectory = outputSegMergeInfos[0].directory;
    assert(mMergeHint.GetId() != MergeItemHint::INVALID_PARALLEL_MERGE_ITEM_ID);
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
    try {
        file_system::FileWriterPtr fileWriter = mergeDirectory->CreateFileWriter(baseItem->mFileName);
        string fileJsonStr = autil::legacy::ToJsonString(baseItem->mDataMap);
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

        file_system::FileWriterPtr fileWriter = mergeDirectory->CreateFileWriter(DemoIndexer::META_FILE_NAME);
        string fileJsonStr = autil::legacy::ToJsonString(mMeta);
        fileWriter->Write(fileJsonStr.data(), fileJsonStr.size()).GetOrThrow();
        fileWriter->Close().GetOrThrow();
    } catch (const autil::legacy::ExceptionBase& e) {
        IE_LOG(ERROR, "fail to store meta file [%s], error: [%s]", DemoIndexer::META_FILE_NAME.c_str(), e.what());
        return false;
    }
    return true;
}

int64_t
DemoIndexReducer::EstimateMemoryUse(const std::vector<file_system::DirectoryPtr>& indexDirs,
                                    const index_base::SegmentMergeInfos& segmentInfos,
                                    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
                                    bool isSortMerge) const
{
    int64_t totalFileLength = 0;
    for (const auto& dir : indexDirs) {
        totalFileLength += dir->GetFileLength("demo_index_file");
    }
    return totalFileLength * 2;
}
}} // namespace indexlib::index
