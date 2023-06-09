/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_index_merger.h"

#include "indexlib/config/range_index_config.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeInfo.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_accessory_reader.h"
#include "indexlib/index/normal/util/segment_file_compress_util.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(legacy, RangeIndexMerger);

void RangeLevelIndexMerger::PrepareIndexOutputSegmentResource(
    const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
    const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    bool needCreateBitmapIndex = mIndexConfig->GetHighFreqVocabulary() != nullptr;
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
        const file_system::DirectoryPtr& indexDirectory = outputSegMergeInfos[i].directory;
        const file_system::DirectoryPtr& parentDirector = indexDirectory->MakeDirectory(mParentIndexName);
        const file_system::DirectoryPtr& mergeDir =
            MakeMergeDir(parentDirector, mIndexConfig, mIndexFormatOption, mMergeHint);
        IndexOutputSegmentResourcePtr outputResource(new IndexOutputSegmentResource);
        outputResource->Init(mergeDir, mIndexConfig, mIOConfig, outputSegMergeInfos[i].temperatureLayer, &mSimplePool,
                             needCreateBitmapIndex);
        mIndexOutputSegmentResources.push_back(outputResource);
    }
}

void RangeIndexMerger::Init(const config::IndexConfigPtr& indexConfig, const index_base::MergeItemHint& hint,
                            const index_base::MergeTaskResourceVector& taskResources,
                            const config::MergeIOConfig& ioConfig, TruncateIndexWriterCreator* truncateCreator,
                            AdaptiveBitmapIndexWriterCreator* adaptiveCreator)
{
    mIndexConfig = indexConfig;
    RangeIndexConfigPtr rangeIndexConfig = DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
    IndexConfigPtr bottomLevelConfig = rangeIndexConfig->GetBottomLevelIndexConfig();
    mBottomLevelIndexMerger->Init(indexConfig->GetIndexName(), bottomLevelConfig, hint, mTaskResources, ioConfig,
                                  truncateCreator, adaptiveCreator);

    IndexConfigPtr highLevelConfig = rangeIndexConfig->GetHighLevelIndexConfig();
    mHighLevelIndexMerger->Init(indexConfig->GetIndexName(), highLevelConfig, hint, taskResources, ioConfig,
                                truncateCreator, adaptiveCreator);
}

void RangeIndexMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir)
{
    NormalIndexMerger::BeginMerge(segDir);
    mBottomLevelIndexMerger->BeginMerge(segDir);
    mHighLevelIndexMerger->BeginMerge(segDir);
}

void RangeIndexMerger::Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                             const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    mSortMerge = false;
    PrepareIndexOutputSegmentResource(resource, segMergeInfos, outputSegMergeInfos);
    mBottomLevelIndexMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);
    mHighLevelIndexMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);
    MergeRangeInfo(segMergeInfos, outputSegMergeInfos);
}

void RangeIndexMerger::PrepareIndexOutputSegmentResource(const index::MergerResource& resource,
                                                         const index_base::SegmentMergeInfos& segMergeInfos,
                                                         const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
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

int64_t RangeIndexMerger::GetMaxLengthOfPosting(const index_base::SegmentData& segData) const
{
    string indexName = mIndexConfig->GetIndexName();
    file_system::DirectoryPtr rootDirectory = segData.GetIndexDirectory(indexName, true);
    file_system::DirectoryPtr indexDirectory =
        rootDirectory->GetDirectory(config::RangeIndexConfig::GetHighLevelIndexName(indexName), true);

    DictionaryReader* dictReader = DictionaryCreator::CreateDiskReader(mIndexConfig);
    unique_ptr<DictionaryReader> dictReaderPtr(dictReader);
    bool supportFileCompress = SegmentFileCompressUtil::IndexSupportFileCompress(mIndexConfig, segData);
    auto status = dictReaderPtr->Open(indexDirectory, DICTIONARY_FILE_NAME, supportFileCompress);
    THROW_IF_STATUS_ERROR(status);
    std::shared_ptr<DictionaryIterator> dictIter = dictReader->CreateIterator();

    int64_t lastOffset = 0;
    int64_t maxLen = 0;
    index::DictKeyInfo key;
    dictvalue_t value;
    while (dictIter->HasNext()) {
        dictIter->Next(key, value);
        int64_t offset = 0;
        if (!ShortListOptimizeUtil::GetOffset(value, offset)) {
            continue;
        }
        maxLen = std::max(maxLen, offset - lastOffset);
        lastOffset = offset;
    }

    size_t indexFileLength = 0;
    CompressFileInfoPtr compressInfo = indexDirectory->GetCompressFileInfo(POSTING_FILE_NAME);
    if (compressInfo) {
        indexFileLength = compressInfo->deCompressFileLen;
    } else {
        indexFileLength = indexDirectory->GetFileLength(POSTING_FILE_NAME);
    }
    maxLen = std::max(maxLen, (int64_t)indexFileLength - lastOffset);
    return maxLen;
}

void RangeIndexMerger::SetReadBufferSize(uint32_t bufSize)
{
    mIOConfig.readBufferSize = bufSize;
    mBottomLevelIndexMerger->SetReadBufferSize(bufSize);
    mHighLevelIndexMerger->SetReadBufferSize(bufSize);
}

void RangeIndexMerger::MergeRangeInfo(const SegmentMergeInfos& segMergeInfos,
                                      const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    RangeInfo info;
    index_base::PartitionDataPtr partitionData = mSegmentDirectory->GetPartitionData();
    assert(partitionData);
    for (size_t i = 0; i < segMergeInfos.size(); ++i) {
        index_base::SegmentData segData = partitionData->GetSegmentData(segMergeInfos[i].segmentId);
        file_system::DirectoryPtr indexDirectory = segData.GetIndexDirectory(mIndexConfig->GetIndexName(), false);
        if (!indexDirectory || !indexDirectory->IsExist(RANGE_INFO_FILE_NAME)) {
            continue;
        }
        RangeInfo tmp;
        auto status = tmp.Load(indexDirectory->GetIDirectory());
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "RangeInfo file load failed. [%s]",
                                 indexDirectory->GetPhysicalPath("").c_str());
        }
        if (tmp.IsValid()) {
            info.Update(tmp.GetMinNumber());
            info.Update(tmp.GetMaxNumber());
        }
    }
    for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
        const file_system::DirectoryPtr& indexDir = outputSegMergeInfos[i].directory;
        const file_system::DirectoryPtr& mergeDir = GetMergeDir(indexDir, false);
        mergeDir->Store(RANGE_INFO_FILE_NAME, autil::legacy::ToJsonString(info));
    }
}
}}} // namespace indexlib::index::legacy
