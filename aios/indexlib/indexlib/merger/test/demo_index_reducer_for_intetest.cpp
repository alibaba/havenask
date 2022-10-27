#include "indexlib/merger/test/demo_index_reducer.h"
#include "indexlib/merger/test/demo_index_reduce_item.h"
#include "indexlib/merger/test/demo_indexer.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/buffered_file_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <autil/legacy/jsonizable.h>
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, DemoIndexReducer);

IndexReduceItemPtr DemoIndexReducer::CreateReduceItem()
{
    DemoIndexReduceItem* item = new DemoIndexReduceItem();
    item->SetMergeTaskResources(mTaskResources);
    IndexReduceItemPtr reduceItem(item);
    return reduceItem;
}

bool DemoIndexReducer::DoInit(const IndexerResourcePtr& resource)
{
    mMergedItemCounter = resource->GetStateCounter("DemoIndexReducer.mergedItemCount");
    mMergedItemCounter->Set(0);
    mMeta["plugin_path"] = resource->pluginPath;     
    return true;
}

bool DemoIndexReducer::Reduce(
        const vector<IndexReduceItemPtr>& reduceItems,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos        ,
        bool isSortMerge,
        const ReduceResource& resource)
{
    assert(outputSegMergeInfos.size() == 1u);
    const auto& mergeDirectory = outputSegMergeInfos[0].directory;
    if (reduceItems.empty())
    {
        IE_LOG(WARN, "reduceItems is empty, nothing to do");
        return IndexReduceItemPtr();
    }
    auto baseItem = DYNAMIC_POINTER_CAST(DemoIndexReduceItem, reduceItems[0]);
    int64_t totalItemCount = baseItem->mDataMap.size();
    for (size_t i = 1; i < reduceItems.size(); ++i)
    {
        auto item = DYNAMIC_POINTER_CAST(DemoIndexReduceItem, reduceItems[i]);
        for (auto iter = item->Begin(); iter != item->End(); ++iter)
        {
            if (baseItem->Add(iter->first, iter->second)) {
                totalItemCount++;
            }
        }
    }
    mMergedItemCounter->Set(totalItemCount);
    try
    {
        string fileJsonStr =  autil::legacy::ToJsonString(baseItem->mDataMap);
        const FileWriterPtr& fileWriter =
            mergeDirectory->CreateFileWriter(baseItem->mFileName);
        fileWriter->Write(fileJsonStr.data(), fileJsonStr.size());
        fileWriter->Close();
    }
    catch(const autil::legacy::ExceptionBase& e)
    {
        IE_LOG(ERROR, "fail to store index file [%s], error: [%s]",
               baseItem->mFileName.c_str(), e.what());
        return false;
    }

    try
    {
        mMeta["segment_type"] = "merge_segment";
        if (resource.isEntireDataSet)
        {
            mMeta["is_complete_merge"] = "true"; 
        }
        else
        {
            mMeta["is_complete_merge"] = "false"; 
        }
        
        string fileJsonStr =  autil::legacy::ToJsonString(mMeta);
        const FileWriterPtr& fileWriter =
            mergeDirectory->CreateFileWriter(DemoIndexer::META_FILE_NAME);
        fileWriter->Write(fileJsonStr.data(), fileJsonStr.size());
        fileWriter->Close();        
    }
    catch(const autil::legacy::ExceptionBase& e)
    {
        IE_LOG(ERROR, "fail to store meta file [%s], error: [%s]",
               DemoIndexer::META_FILE_NAME.c_str(), e.what());
        return false;
    }
    return true;
}

int64_t DemoIndexReducer::EstimateMemoryUse(
        const vector<DirectoryPtr>& indexDirs,
        const SegmentMergeInfos& segmentMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, // target segments
        bool isSortMerge) const
{
    int64_t totalFileLength = 0;
    for (const auto dir : indexDirs)
    {
        totalFileLength += dir->GetFileLength("demo_index_file");
    }
    return totalFileLength * 2;
}

vector<ReduceTask> DemoIndexReducer::CreateReduceTasks(const vector<DirectoryPtr>& indexDirs,
    const SegmentMergeInfos& segmentInfos, uint32_t instanceCount, bool isEntireDataSet,
    MergeTaskResourceManagerPtr& resMgr)
{
    map<int32_t, int32_t> categoryCountMap;
    int32_t totalDocCount = 0;
    for (auto builtSegDir : indexDirs)
    {
        string fileContent;
        try
        {
            builtSegDir->Load("demo_index_file", fileContent);
            typedef std::pair<docid_t, std::string> DocItem;
            vector<DocItem> docItemVec;
            autil::legacy::FromJsonString(docItemVec, fileContent);
            for (const auto docItem : docItemVec)
            {
                vector<string> docData = StringTokenizer::tokenize(
                    ConstString(docItem.second), ",");
                if (docData.size() < 3)
                {
                    continue;
                }
                int32_t catId;
                if (!StringUtil::fromString(docData[2], catId))
                {
                    continue;
                }
                auto iter = categoryCountMap.find(catId);
                if (iter == categoryCountMap.end())
                {
                    categoryCountMap.insert(make_pair(catId, 1));
                }
                else
                {
                    categoryCountMap[catId] = iter->second + 1;
                }
                totalDocCount += 1;
            }
        }

        catch(const autil::legacy::ExceptionBase& e)
        {
            INDEXLIB_THROW(misc::NonExistException, "fail to load index file [demo_index_file]");
        }
    }

    stringstream task1CatId;
    stringstream task2CatId;
    vector<ReduceTask> reduceTasks;
    reduceTasks.push_back(ReduceTask());
    reduceTasks[0].id = 0;
    reduceTasks.push_back(ReduceTask());
    reduceTasks[1].id = 1;
    int32_t tempCatCount = 0;
    // only for test use
    for (auto it = categoryCountMap.begin(); it != categoryCountMap.end(); it++)
    {
        if (tempCatCount + it->second <= totalDocCount / 2)
        {
            task1CatId << it->first << ",";
            tempCatCount += it->second;
        }
        else
        {
            task2CatId << it->first << ",";
        }

    }

    reduceTasks[0].dataRatio = (totalDocCount > 0) ?
                               (float)tempCatCount / totalDocCount : 1;
    reduceTasks[1].dataRatio = 1 - reduceTasks[0].dataRatio;
    
    resourceid_t resId1 = resMgr->DeclareResource(task1CatId.str().c_str(),
                                                  task1CatId.str().size() - 1);
    reduceTasks[0].resourceIds.push_back(resId1);
    resourceid_t resId2 = resMgr->DeclareResource(task2CatId.str().c_str(),
                                                  task2CatId.str().size() - 1);
    reduceTasks[1].resourceIds.push_back(resId2);
    return reduceTasks;
}

void DemoIndexReducer::EndParallelReduce(
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos, int32_t totalParallelCount,
    const vector<MergeTaskResourceVector>& instResourceVec)
{
    for (const auto& info : outputSegMergeInfos)
    {
        DemoParallelReduceMeta meta(totalParallelCount);
        meta.Store(info.directory);
    }
}

IE_NAMESPACE_END(merger);
