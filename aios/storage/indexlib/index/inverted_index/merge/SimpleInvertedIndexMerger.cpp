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
#include "indexlib/index/inverted_index/merge/SimpleInvertedIndexMerger.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/common/patch/PatchFileInfo.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/PostingMergerImpl.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/SegmentTermInfoQueue.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingMerger.h"
#include "indexlib/index/inverted_index/builtin_index/pack/OnDiskPackIndexIterator.h"
#include "indexlib/index/inverted_index/merge/SimpleDocMapper.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SimpleInvertedIndexMerger);

SimpleInvertedIndexMerger::SimpleInvertedIndexMerger(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
    : _indexConfig(indexConfig)
{
    _indexFormatOption.Init(_indexConfig);

    _allocator = std::make_unique<util::MMapAllocator>();
    _byteSlicePool = std::make_unique<autil::mem_pool::Pool>(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024);
    _bufferPool = std::make_unique<autil::mem_pool::RecyclePool>(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024);
    _postingWriterResource = std::make_unique<PostingWriterResource>(
        &_simplePool, _byteSlicePool.get(), _bufferPool.get(), _indexFormatOption.GetPostingFormatOption());
}

int64_t SimpleInvertedIndexMerger::CurrentMemUse()
{
    return _byteSlicePool->getUsedBytes() + _simplePool.getUsedBytes() + _bufferPool->getUsedBytes();
}

Status SimpleInvertedIndexMerger::Merge(const std::shared_ptr<file_system::Directory>& inputIndexDir,
                                        const std::shared_ptr<indexlibv2::index::PatchFileInfo>& inputPatchFileInfo,
                                        const std::shared_ptr<file_system::Directory>& outputIndexDir,
                                        size_t dictKeyCount, uint64_t docCount)
{
    auto status = DoMerge(inputIndexDir, inputPatchFileInfo, outputIndexDir, dictKeyCount, docCount);

    for (auto& indexOutputSegmentResource : _indexOutputSegmentResources) {
        indexOutputSegmentResource->Reset();
    }
    _indexOutputSegmentResources.clear();
    if (_byteSlicePool) {
        _byteSlicePool->release();
    }
    if (_bufferPool) {
        _bufferPool->release();
    }
    return status;
}

Status SimpleInvertedIndexMerger::DoMerge(const std::shared_ptr<file_system::Directory>& inputIndexDir,
                                          const std::shared_ptr<indexlibv2::index::PatchFileInfo>& inputPatchFileInfo,
                                          const std::shared_ptr<file_system::Directory>& outputIndexDir,
                                          size_t dictKeyCount, uint64_t docCount)
{
    PrepareIndexOutputSegmentResource(outputIndexDir, dictKeyCount);
    // Init term queue
    auto onDiskIndexIterCreator = std::make_shared<OnDiskPackIndexIteratorTyped<void>::Creator>(
        _indexFormatOption.GetPostingFormatOption(), _ioConfig, _indexConfig);
    SegmentTermInfoQueue termInfoQueue(_indexConfig, onDiskIndexIterCreator);
    auto status = termInfoQueue.Init(inputIndexDir, inputPatchFileInfo);
    RETURN_IF_STATUS_ERROR(status, "init term info queue in [%s] faild", inputIndexDir->GetLogicalPath().c_str());

    std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>> targetSegmentMetas;
    segmentid_t segmentId = 0;
    auto segmentMeta = std::make_shared<indexlibv2::framework::SegmentMeta>();
    segmentMeta->segmentId = segmentId;
    segmentMeta->segmentInfo = std::make_shared<indexlibv2::framework::SegmentInfo>();
    segmentMeta->segmentInfo->docCount = docCount;
    targetSegmentMetas.push_back(segmentMeta);
    auto docMapper = std::make_shared<SimpleDocMapper>(segmentId, docCount);

    index::DictKeyInfo key;
    while (!termInfoQueue.Empty()) {
        SegmentTermInfo::TermIndexMode termMode;
        const auto& segTermInfos = termInfoQueue.CurrentTermInfos(key, termMode);
        MergeTerm(key, segTermInfos, docMapper, termMode, targetSegmentMetas);
        termInfoQueue.MoveToNextTerm();
    }
    return Status::OK();
}

void SimpleInvertedIndexMerger::PrepareIndexOutputSegmentResource(
    const std::shared_ptr<file_system::Directory>& outputDir, size_t dictKeyCount)
{
    bool needCreateBitmapIndex = _indexConfig->GetHighFreqVocabulary() != nullptr;
    std::string optionString = IndexFormatOption::ToString(_indexFormatOption);
    outputDir->Store(INDEX_FORMAT_OPTION_FILE_NAME, optionString);
    auto outputResource = std::make_shared<IndexOutputSegmentResource>(dictKeyCount);
    std::shared_ptr<indexlibv2::framework::SegmentStatistics> segmentStatistics;
    outputResource->Init(outputDir, _indexConfig, _ioConfig, segmentStatistics, &_simplePool, needCreateBitmapIndex);
    _indexOutputSegmentResources.push_back(outputResource);
}

void SimpleInvertedIndexMerger::MergeTerm(
    DictKeyInfo key, const SegmentTermInfos& segTermInfos,
    const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper, SegmentTermInfo::TermIndexMode mode,
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
{
    std::shared_ptr<PostingMerger> postingMerger;
    if (mode == SegmentTermInfo::TM_BITMAP) {
        postingMerger.reset(
            new BitmapPostingMerger(_byteSlicePool.get(), targetSegments, _indexConfig->GetOptionFlag()));
    } else {
        postingMerger.reset(new PostingMergerImpl(_postingWriterResource.get(), targetSegments));
    }
    postingMerger->Merge(segTermInfos, docMapper);

    if (postingMerger->GetDocFreq() > 0) {
        postingMerger->Dump(key, _indexOutputSegmentResources);
        _byteSlicePool->reset();
        _bufferPool->reset();
    }
}

} // namespace indexlib::index
