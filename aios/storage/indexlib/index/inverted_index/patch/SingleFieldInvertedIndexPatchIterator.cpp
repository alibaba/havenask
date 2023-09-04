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
#include "indexlib/index/inverted_index/patch/SingleFieldInvertedIndexPatchIterator.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchFileFinder.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SingleFieldInvertedIndexPatchIterator);

SingleFieldInvertedIndexPatchIterator::SingleFieldInvertedIndexPatchIterator(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedIndexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& patchExtraDir)
    : _invertedIndexConfig(invertedIndexConfig)
    , _patchExtraDir(patchExtraDir)
    , _cursor(-1)
    , _patchLoadExpandSize(0)
    , _patchItemCount(0)
{
}

Status SingleFieldInvertedIndexPatchIterator::Init(
    const std::vector<std::shared_ptr<indexlibv2::framework::Segment>>& segments)
{
    std::map<segmentid_t, indexlibv2::index::PatchFileInfos> patchInfos;
    InvertedIndexPatchFileFinder patchFinder;
    auto status = patchFinder.FindAllPatchFiles(segments, _invertedIndexConfig, &patchInfos);
    RETURN_IF_STATUS_ERROR(status, "find all patch files failed for inverted index [%s]", GetIndexName().c_str());

    if (_patchExtraDir) {
        auto [status, indexDir] = patchFinder.GetIndexDirectory(_patchExtraDir, _invertedIndexConfig);
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        if (indexDir) {
            AUTIL_LOG(DEBUG, "get inverted index [%s] patch extra from path [%s]",
                      _invertedIndexConfig->GetIndexName().c_str(), indexDir->DebugString().c_str());
            RETURN_STATUS_DIRECTLY_IF_ERROR(patchFinder.FindPatchFiles(indexDir, INVALID_SEGMENTID, &patchInfos));
        }
    }

    for (const auto& segment : segments) {
        auto targetSegmentId = segment->GetSegmentId();
        auto iter = patchInfos.find(targetSegmentId);
        if (iter == patchInfos.end()) {
            continue;
        }
        const indexlibv2::index::PatchFileInfos& patchFileInfos = iter->second;
        auto segPatchIter =
            std::make_shared<SingleFieldIndexSegmentPatchIterator>(_invertedIndexConfig, targetSegmentId);
        for (size_t i = 0; i < patchFileInfos.Size(); i++) {
            auto status = segPatchIter->AddPatchFile(patchFileInfos[i].patchDirectory, patchFileInfos[i].patchFileName,
                                                     patchFileInfos[i].srcSegment, patchFileInfos[i].destSegment);
            RETURN_IF_STATUS_ERROR(status, "add patch file [%s] failed", patchFileInfos[i].patchFileName.c_str());
        }
        _patchLoadExpandSize += segPatchIter->GetPatchLoadExpandSize();
        _patchItemCount += segPatchIter->GetPatchItemCount();
        _segmentIterators.push_back(segPatchIter);
    }
    if (!_segmentIterators.empty()) {
        _cursor = 0;
        Log();
    }
    return Status::OK();
}

const std::string& SingleFieldInvertedIndexPatchIterator::GetIndexName() const
{
    return _invertedIndexConfig->GetIndexName();
}

void SingleFieldInvertedIndexPatchIterator::Log() const
{
    if (_cursor < _segmentIterators.size()) {
        auto& segIter = _segmentIterators[_cursor];
        AUTIL_LOG(INFO, "index [%s] patch next target segment [%d], patch item[%lu]",
                  _invertedIndexConfig->GetIndexName().c_str(), segIter->GetTargetSegmentId(),
                  segIter->GetPatchItemCount());
    }
}

std::unique_ptr<IndexUpdateTermIterator> SingleFieldInvertedIndexPatchIterator::Next()
{
    if (_cursor < 0) {
        return nullptr;
    }
    while (_cursor < _segmentIterators.size()) {
        auto& segIter = _segmentIterators[_cursor];
        std::unique_ptr<IndexUpdateTermIterator> r(segIter->NextTerm());
        if (r) {
            return r;
        } else {
            ++_cursor;
            Log();
        }
    }
    return nullptr;
}

// void SingleFieldInvertedIndexPatchIterator::CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems)
// {
//     if (_cursor < 0) {
//         return;
//     }
//     for (; _cursor < _segmentIterators.size(); ++_cursor) {
//         const auto& segIter = _segmentIterators[_cursor];
//         std::stringstream ss;
//         ss << "__index__" << _invertedIndexConfig->GetIndexName() << "_" << segIter->GetTargetSegmentId() << "_";
//         if (_isSub) {
//             ss << "sub";
//         } else {
//             ss << "main";
//         }
//         auto workItem =
//             new IndexPatchWorkItem(ss.str(), segIter->GetPatchItemCount(), _isSub, PatchWorkItem::PWIT_INDEX,
//             segIter);
//         workItems.push_back(workItem);
//     }
// }

} // namespace indexlib::index
