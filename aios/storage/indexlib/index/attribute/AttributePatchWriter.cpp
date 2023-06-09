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
#include "indexlib/index/attribute/AttributePatchWriter.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/index/attribute/patch/AttributeUpdaterFactory.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributePatchWriter);

AttributePatchWriter::AttributePatchWriter(std::shared_ptr<indexlib::file_system::IDirectory> workDir,
                                           segmentid_t srcSegmentId,
                                           const std::shared_ptr<config::IIndexConfig>& indexConfig)

    : PatchWriter(workDir, srcSegmentId)
    , _indexConfig(indexConfig)
{
}

Status AttributePatchWriter::Write(segmentid_t targetSegmentId, docid_t localDocId, std::string_view value, bool isNull)
{
    std::lock_guard<std::mutex> lock(_writeMutex);
    assert(targetSegmentId != _srcSegmentId);

    if (_segmentId2Updaters.find(targetSegmentId) == _segmentId2Updaters.end()) {
        std::unique_ptr<AttributeUpdater> updater =
            AttributeUpdaterFactory::GetInstance()->CreateAttributeUpdater(targetSegmentId, _indexConfig);
        _segmentId2Updaters.insert({targetSegmentId, std::move(updater)});
    }
    _segmentId2Updaters.at(targetSegmentId)->Update(localDocId, value, isNull);
    return Status::OK();
}

Status AttributePatchWriter::Close()
{
    for (const auto& pair : _segmentId2Updaters) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(pair.second->Dump(_workDir, _srcSegmentId));
    }
    return Status::OK();
}

Status AttributePatchWriter::GetPatchFileInfos(PatchFileInfos* patchFileInfos)
{
    for (const auto& pair : _segmentId2Updaters) {
        PatchFileInfo patchFileInfo(_srcSegmentId, pair.first, _workDir, pair.second->GetPatchFileName());
        patchFileInfos->PushBack(patchFileInfo);
    }
    return Status::OK();
}

} // namespace indexlibv2::index
