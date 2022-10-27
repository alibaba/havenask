#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_merger.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/segment_directory_base.h"
#include "indexlib/config/range_index_config.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangeIndexMerger);

void RangeLevelIndexMerger::PrepareIndexOutputSegmentResource(
    const index::MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i)
    {
        const file_system::DirectoryPtr& indexDirectory = outputSegMergeInfos[i].directory;
        const file_system::DirectoryPtr& parentDirector = indexDirectory->MakeDirectory(mParentIndexName);
        const file_system::DirectoryPtr& mergeDir = MakeMergeDir(parentDirector,
                mIndexConfig, mIndexFormatOption, mMergeHint);
        IndexOutputSegmentResourcePtr outputResource(new IndexOutputSegmentResource);
        outputResource->Init(mergeDir, mIndexConfig, mIOConfig, &mSimplePool, false);
        mIndexOutputSegmentResources.push_back(outputResource);
    }
}

void RangeIndexMerger::Init(const config::IndexConfigPtr& indexConfig,
                            const index_base::MergeItemHint& hint,
                            const index_base::MergeTaskResourceVector& taskResources,
                            const config::MergeIOConfig& ioConfig,
                            TruncateIndexWriterCreator* truncateCreator,
                            AdaptiveBitmapIndexWriterCreator* adaptiveCreator)
{
    mIndexConfig = indexConfig;
    RangeIndexConfigPtr rangeIndexConfig =
        DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    IndexConfigPtr bottomLevelConfig =
        rangeIndexConfig->GetBottomLevelIndexConfig();
    mBottomLevelIndexMerger->Init(indexConfig->GetIndexName(), bottomLevelConfig,
                                  hint, mTaskResources, ioConfig, truncateCreator, adaptiveCreator);

    IndexConfigPtr highLevelConfig = rangeIndexConfig->GetHighLevelIndexConfig();
    mHighLevelIndexMerger->Init(indexConfig->GetIndexName(), highLevelConfig,
                                hint, taskResources, ioConfig, truncateCreator, adaptiveCreator);    
}

void RangeIndexMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir)
{
    IndexMerger::BeginMerge(segDir);
    mBottomLevelIndexMerger->BeginMerge(segDir);
    mHighLevelIndexMerger->BeginMerge(segDir);
}

void RangeIndexMerger::Merge(
    const index::MergerResource& resource,
    const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    mSortMerge = false;
    PrepareIndexOutputSegmentResource(resource, segMergeInfos, outputSegMergeInfos);
    mBottomLevelIndexMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);
    mHighLevelIndexMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);
    MergeRangeInfo(segMergeInfos, outputSegMergeInfos);
}

void RangeIndexMerger::PrepareIndexOutputSegmentResource(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,        
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) 
{
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) 
    {
        file_system::DirectoryPtr indexDirectory = outputSegMergeInfos[i].directory;
        GetMergeDir(indexDirectory, true);
    }
}

void RangeIndexMerger::SortByWeightMerge(const index::MergerResource& resource,
                                         const index_base::SegmentMergeInfos& segMergeInfos,
                                         const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    mSortMerge = true;
    PrepareIndexOutputSegmentResource(resource, segMergeInfos, outputSegMergeInfos);
    mBottomLevelIndexMerger->SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
    mHighLevelIndexMerger->SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
    MergeRangeInfo(segMergeInfos, outputSegMergeInfos);
}

int64_t RangeIndexMerger::GetMaxLengthOfPosting(
        const index_base::SegmentData& segData) const
{
    string indexName = mIndexConfig->GetIndexName();
    file_system::DirectoryPtr rootDirectory = 
        segData.GetIndexDirectory(indexName, true);
    file_system::DirectoryPtr indexDirectory = 
        rootDirectory->GetDirectory(
                config::RangeIndexConfig::GetHighLevelIndexName(indexName), true);

    DictionaryReader *dictReader = DictionaryCreator::CreateDiskReader(mIndexConfig);
    unique_ptr<DictionaryReader> dictReaderPtr(dictReader);
    dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME);
    DictionaryIteratorPtr dictIter = dictReader->CreateIterator();

    int64_t lastOffset = 0;
    int64_t maxLen = 0;
    dictkey_t key;
    dictvalue_t value;
    while (dictIter->HasNext())
    {
        dictIter->Next(key, value);
        int64_t offset = 0;
        if (!ShortListOptimizeUtil::GetOffset(value, offset))
        {
            continue;
        }
        maxLen = std::max(maxLen, offset - lastOffset);
        lastOffset = offset;
    }
    size_t indexFileLength = indexDirectory->GetFileLength(POSTING_FILE_NAME);
    maxLen = std::max(maxLen, (int64_t)indexFileLength - lastOffset);
    return maxLen;    
}

void RangeIndexMerger::SetReadBufferSize(uint32_t bufSize)
{
    mIOConfig.readBufferSize = bufSize;
    mBottomLevelIndexMerger->SetReadBufferSize(bufSize);
    mHighLevelIndexMerger->SetReadBufferSize(bufSize);
}

void RangeIndexMerger::MergeRangeInfo(
    const SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    RangeInfo info;
    index_base::PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);
    for (size_t i = 0; i < segMergeInfos.size(); ++i)
    {
        index_base::SegmentData segData = partitionData->GetSegmentData(segMergeInfos[i].segmentId);
        file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(
                mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(RANGE_INFO_FILE_NAME))
        {
            continue;
        }
        RangeInfo tmp;
        tmp.Load(indexDirectory);
        if (tmp.IsValid())
        {
            info.Update(tmp.GetMinNumber());
            info.Update(tmp.GetMaxNumber());
        }
    }
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i)
    {
        const file_system::DirectoryPtr& indexDir = outputSegMergeInfos[i].directory;
        const file_system::DirectoryPtr& mergeDir = GetMergeDir(indexDir, false);
        mergeDir->Store(RANGE_INFO_FILE_NAME, autil::legacy::ToJsonString(info));
    }
}

IE_NAMESPACE_END(index);

