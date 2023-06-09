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
#include "indexlib/index/inverted_index/SegmentTermInfoQueue.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/DefaultValueIndexIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/OnDiskBitmapIndexIterator.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchFileFinder.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"

namespace indexlib::index {
namespace {
using indexlibv2::index::IIndexMerger;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, SegmentTermInfoQueue);

SegmentTermInfoQueue::SegmentTermInfoQueue(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                           const std::shared_ptr<OnDiskIndexIteratorCreator>& onDiskIndexIterCreator)
    : _indexConfig(indexConfig)
    , _onDiskIndexIterCreator(onDiskIndexIterCreator)
{
}

SegmentTermInfoQueue::~SegmentTermInfoQueue()
{
    while (!_segmentTermInfos.empty()) {
        SegmentTermInfo* segTermInfo = _segmentTermInfos.top();
        _segmentTermInfos.pop();
        delete segTermInfo;
    }
    for (size_t i = 0; i < _mergingSegmentTermInfos.size(); i++) {
        DELETE_AND_SET_NULL(_mergingSegmentTermInfos[i]);
    }
}

Status SegmentTermInfoQueue::Init(const std::vector<IIndexMerger::SourceSegment>& srcSegments,
                                  const indexlibv2::index::PatchInfos& patchInfos)
{
    AUTIL_LOG(INFO, "init SegmentTermInfoQueue for [%s] with [%lu] segment", _indexConfig->GetIndexName().c_str(),
              srcSegments.size());
    _patchInfos = patchInfos;

    for (size_t i = 0; i < srcSegments.size(); ++i) {
        auto status =
            AddOnDiskTermInfo(srcSegments[i].baseDocid, srcSegments[i].segment->GetSegmentInfo()->docCount,
                              srcSegments[i].segment->GetSegmentId(), srcSegments[i].segment->GetSegmentSchema(),
                              srcSegments[i].segment->GetSegmentDirectory());
        RETURN_IF_STATUS_ERROR(status, "load segment [%d] term info failed", srcSegments[i].segment->GetSegmentId());
    }
    return Status::OK();
}

Status SegmentTermInfoQueue::AddOnDiskTermInfo(docid_t baseDocId, uint64_t docCount, segmentid_t segmentId,
                                               const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                                               const std::shared_ptr<file_system::Directory>& segmentDir)
{
    auto [status, patchIter] = CreatePatchIterator(segmentId);
    RETURN_IF_STATUS_ERROR(status, "create patch iterator for segment [%d] failed", segmentId);
    std::shared_ptr<IndexIterator> indexIt = CreateOnDiskNormalIterator(segmentDir, /*indexDir=*/nullptr);
    if (indexIt == nullptr && schema) {
        indexIt = CreateForDefaultValueIter(schema, docCount, segmentId);
    }
    AddQueueItem(baseDocId, segmentId, indexIt, patchIter, SegmentTermInfo::TM_NORMAL);

    if (_indexConfig->GetHighFreqVocabulary()) {
        std::tie(status, patchIter) = CreatePatchIterator(segmentId);
        RETURN_IF_STATUS_ERROR(status, "create patch iterator for segment [%d] failed", segmentId);
        indexIt = CreateOnDiskBitmapIterator(GetIndexDir(segmentDir));
        AddQueueItem(baseDocId, segmentId, indexIt, patchIter, SegmentTermInfo::TM_BITMAP);
    }
    return Status::OK();
}

Status SegmentTermInfoQueue::Init(const std::shared_ptr<file_system::Directory>& indexDir,
                                  const std::shared_ptr<indexlibv2::index::PatchFileInfo>& patchFileInfo)
{
    size_t baseDocId = 0;
    segmentid_t segmentId = 0;

    if (patchFileInfo) {
        _patchInfos[segmentId] = indexlibv2::index::PatchFileInfos();
        _patchInfos[segmentId].PushBack(*patchFileInfo);
    }

    auto [status, patchIter] = CreatePatchIterator(segmentId);
    RETURN_IF_STATUS_ERROR(status, "create patch iterator for segment [%d] failed", segmentId);
    std::shared_ptr<IndexIterator> indexIt = CreateOnDiskNormalIterator(/* segmentDir */ nullptr, indexDir);
    assert(indexIt);
    AddQueueItem(baseDocId, segmentId, indexIt, patchIter, SegmentTermInfo::TM_NORMAL);

    if (_indexConfig->GetHighFreqVocabulary()) {
        std::tie(status, patchIter) = CreatePatchIterator(segmentId);
        RETURN_IF_STATUS_ERROR(status, "create patch iterator for segment [%d] failed", segmentId);
        indexIt = CreateOnDiskBitmapIterator(indexDir);
        AddQueueItem(baseDocId, segmentId, indexIt, patchIter, SegmentTermInfo::TM_BITMAP);
    }
    return Status::OK();
}

std::shared_ptr<IndexIterator>
SegmentTermInfoQueue::CreateForDefaultValueIter(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                                                uint64_t docCount, segmentid_t segmentId) const
{
    if (schema->GetIndexConfig(INVERTED_INDEX_TYPE_STR, _indexConfig->GetIndexName()) == nullptr) {
        std::shared_ptr<IndexIterator> ret(new DefaultValueIndexIterator(_indexConfig, docCount));
        AUTIL_LOG(INFO, "src segment [%d] use default value iter for index [%s]", segmentId,
                  _indexConfig->GetIndexName().c_str());
        return ret;
    }
    return nullptr;
}

void SegmentTermInfoQueue::AddQueueItem(size_t baseDocid, segmentid_t& segmentId,
                                        const std::shared_ptr<IndexIterator>& indexIt,
                                        const std::shared_ptr<SingleFieldIndexSegmentPatchIterator>& patchIter,
                                        SegmentTermInfo::TermIndexMode mode)
{
    if (indexIt || (patchIter && !patchIter->Empty())) {
        auto queItem = std::make_unique<SegmentTermInfo>(_indexConfig->GetInvertedIndexType(), segmentId, baseDocid,
                                                         indexIt, patchIter, mode);
        if (queItem->Next()) {
            _segmentTermInfos.push(queItem.release());
        }
    }
}

const std::vector<SegmentTermInfo*>& SegmentTermInfoQueue::CurrentTermInfos(indexlib::index::DictKeyInfo& key,
                                                                            SegmentTermInfo::TermIndexMode& termMode)
{
    assert(_mergingSegmentTermInfos.empty());
    SegmentTermInfo* item = _segmentTermInfos.top();
    _segmentTermInfos.pop();

    key = item->GetKey();
    termMode = item->GetTermIndexMode();

    _mergingSegmentTermInfos.push_back(item);

    item = (_segmentTermInfos.size() > 0) ? _segmentTermInfos.top() : nullptr;
    while (item != nullptr && key == item->GetKey()) {
        if (item->GetTermIndexMode() != termMode) {
            break;
        }
        _mergingSegmentTermInfos.push_back(item);
        _segmentTermInfos.pop();
        item = (_segmentTermInfos.size() > 0) ? _segmentTermInfos.top() : nullptr;
    }
    return _mergingSegmentTermInfos;
}

void SegmentTermInfoQueue::MoveToNextTerm()
{
    for (size_t i = 0; i < _mergingSegmentTermInfos.size(); ++i) {
        SegmentTermInfo* itemPtr = _mergingSegmentTermInfos[i];
        assert(itemPtr);
        if (itemPtr->Next()) {
            _mergingSegmentTermInfos[i] = nullptr;
            _segmentTermInfos.push(itemPtr);
        } else {
            DELETE_AND_SET_NULL(_mergingSegmentTermInfos[i]);
        }
    }
    _mergingSegmentTermInfos.clear();
}

std::shared_ptr<IndexIterator>
SegmentTermInfoQueue::CreateOnDiskNormalIterator(const std::shared_ptr<file_system::Directory>& segmentDir,
                                                 const std::shared_ptr<file_system::Directory>& indexDir) const
{
    OnDiskIndexIterator* onDiskIndexIter = nullptr;
    if (indexDir) {
        onDiskIndexIter = _onDiskIndexIterCreator->CreateByIndexDir(indexDir);
    } else {
        onDiskIndexIter = _onDiskIndexIterCreator->Create(segmentDir);
    }
    std::shared_ptr<IndexIterator> indexIt;
    if (onDiskIndexIter) {
        indexIt.reset(onDiskIndexIter);
        onDiskIndexIter->Init();
    }
    return indexIt;
}

std::shared_ptr<indexlib::file_system::Directory>
SegmentTermInfoQueue::GetIndexDir(const std::shared_ptr<file_system::Directory>& segmentDir) const
{
    auto subDir = segmentDir->GetDirectory(INVERTED_INDEX_PATH, /*throwExceptionIfNotExist*/ false);
    assert(subDir);
    return subDir->GetDirectory(_indexConfig->GetIndexName(), /*throwExceptionIfNotExist*/ false);
}

std::shared_ptr<IndexIterator>
SegmentTermInfoQueue::CreateOnDiskBitmapIterator(const std::shared_ptr<file_system::Directory>& indexDir) const
{
    std::shared_ptr<IndexIterator> indexIt;
    if (indexDir && indexDir->IsExist(BITMAP_DICTIONARY_FILE_NAME)) {
        OnDiskIndexIterator* onDiskIndexIter = _onDiskIndexIterCreator->CreateBitmapIterator(indexDir);
        indexIt.reset(onDiskIndexIter);
        onDiskIndexIter->Init();
    }
    return indexIt;
}

std::pair<Status, std::shared_ptr<SingleFieldIndexSegmentPatchIterator>>
SegmentTermInfoQueue::CreatePatchIterator(segmentid_t targetSegmentId) const
{
    std::shared_ptr<SingleFieldIndexSegmentPatchIterator> patchIter(
        new SingleFieldIndexSegmentPatchIterator(_indexConfig, targetSegmentId));
    auto iter = _patchInfos.find(targetSegmentId);
    if (iter != _patchInfos.end()) {
        const indexlibv2::index::PatchFileInfos& patchFileInfos = iter->second;
        for (size_t i = 0; i < patchFileInfos.Size(); ++i) {
            auto status = patchIter->AddPatchFile(patchFileInfos[i].patchDirectory, patchFileInfos[i].patchFileName,
                                                  patchFileInfos[i].srcSegment, patchFileInfos[i].destSegment);
            RETURN2_IF_STATUS_ERROR(status, nullptr, "add patch file [%s] failed",
                                    patchFileInfos[i].patchFileName.c_str());
        }
    }
    return {Status::OK(), patchIter};
}

} // namespace indexlib::index
