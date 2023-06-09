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
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchWriter.h"

#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexSegmentUpdater.h"
#include "indexlib/index/inverted_index/patch/MultiShardInvertedIndexSegmentUpdater.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::config::InvertedIndexConfig;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, InvertedIndexPatchWriter);

InvertedIndexPatchWriter::InvertedIndexPatchWriter(std::shared_ptr<file_system::IDirectory> workDir,
                                                   segmentid_t srcSegmentId,
                                                   const std::shared_ptr<IIndexConfig>& indexConfig)
    : PatchWriter(workDir, srcSegmentId)
{
    _indexConfig = std::dynamic_pointer_cast<InvertedIndexConfig>(indexConfig);
    assert(_indexConfig);
    // 与V1版本相比少了util::BuildResourceMetrics，这是因为V2中PatchWriter由离线OpLog2Patch任务调用执行，与Attribute行为一致
}

Status InvertedIndexPatchWriter::Write(segmentid_t targetSegmentId, docid_t localDocId, index::DictKeyInfo termKey,
                                       bool isDelete)
{
    std::lock_guard<std::mutex> lock(_writeMutex);
    assert(targetSegmentId != _srcSegmentId);

    if (_segmentId2Updaters.find(targetSegmentId) == _segmentId2Updaters.end()) {
        if (_indexConfig->GetShardingType() == InvertedIndexConfig::IST_NO_SHARDING) {
            std::unique_ptr<IInvertedIndexSegmentUpdater> updater(
                new InvertedIndexSegmentUpdater(targetSegmentId, _indexConfig));
            _segmentId2Updaters.insert({targetSegmentId, std::move(updater)});
        } else if (_indexConfig->GetShardingType() == InvertedIndexConfig::IST_NEED_SHARDING) {
            std::unique_ptr<IInvertedIndexSegmentUpdater> updater(
                new MultiShardInvertedIndexSegmentUpdater(targetSegmentId, _indexConfig));
            _segmentId2Updaters.insert({targetSegmentId, std::move(updater)});
        } else {
            assert(false);
            return Status::Unimplement();
        }
    }
    _segmentId2Updaters.at(targetSegmentId)->Update(localDocId, termKey, isDelete);
    return Status::OK();
}

Status InvertedIndexPatchWriter::Close()
{
    for (const auto& pair : _segmentId2Updaters) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(pair.second->Dump(_workDir, _srcSegmentId));
    }
    return Status::OK();
}

Status InvertedIndexPatchWriter::GetPatchFileInfos(indexlibv2::index::PatchFileInfos* patchFileInfos)
{
    for (const auto& pair : _segmentId2Updaters) {
        indexlibv2::index::PatchFileInfo patchFileInfo(_srcSegmentId, pair.first, _workDir,
                                                       pair.second->GetPatchFileName(_srcSegmentId, pair.first));
        patchFileInfos->PushBack(patchFileInfo);
    }
    return Status::OK();
}

} // namespace indexlib::index
