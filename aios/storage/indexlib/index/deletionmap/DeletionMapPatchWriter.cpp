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
#include "indexlib/index/deletionmap/DeletionMapPatchWriter.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapPatchWriter);

DeletionMapPatchWriter::DeletionMapPatchWriter(std::shared_ptr<indexlib::file_system::IDirectory> workDir,
                                               const std::map<segmentid_t, size_t>& segmentId2DocCount)
    : PatchWriter(workDir, INVALID_SEGMENTID)
    , _segmentId2DocCount(segmentId2DocCount)
{
}

Status DeletionMapPatchWriter::Write(segmentid_t targetSegmentId, docid_t localDocId)
{
    std::lock_guard<std::mutex> lock(_writeMutex);
    if (unlikely(targetSegmentId == _srcSegmentId)) {
        AUTIL_LOG(ERROR, "src segmentId [%d] and target segment id [%d] should not be same", _srcSegmentId,
                  targetSegmentId);
        return Status::InternalError();
    }

    if (_segmentId2Bitmaps.find(targetSegmentId) == _segmentId2Bitmaps.end()) {
        if (unlikely(_segmentId2DocCount.find(targetSegmentId) == _segmentId2DocCount.end())) {
            AUTIL_LOG(ERROR, "can not find segment [%d] doc count", targetSegmentId);
            return Status::InternalError("Can not find segment ", targetSegmentId, " doc count");
        }
        _segmentId2Bitmaps[targetSegmentId] = std::make_shared<indexlib::util::ExpandableBitmap>(
            _segmentId2DocCount[targetSegmentId], /*bSet=*/false, /*pool=*/nullptr);
    }
    _segmentId2Bitmaps.at(targetSegmentId)->Set(localDocId);
    return Status::OK();
}

Status DeletionMapPatchWriter::Close()
{
    assert(_workDir);
    for (const auto& [targetSegmentId, bitmap] : _segmentId2Bitmaps) {
        std::string fileName = DeletionMapUtil::GetDeletionMapFileName(targetSegmentId);
        auto [status, writer] =
            _workDir->CreateFileWriter(fileName, indexlib::file_system::WriterOption()).StatusWith();
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
        RETURN_STATUS_DIRECTLY_IF_ERROR(DeletionMapUtil::DumpBitmap(writer, bitmap.get(), bitmap->GetValidItemCount()));
        auto ret = writer->Close();
        if (!ret.OK()) {
            AUTIL_LOG(ERROR, "File writer close fail [%s]", writer->GetLogicalPath().c_str());
            return Status::IOError("close file writer failed");
        }
    }
    return Status::OK();
}

int64_t DeletionMapPatchWriter::EstimateMemoryUse(const std::map<segmentid_t, size_t>& segmentId2DocCount)
{
    int64_t totalSize = 0;
    for (const auto [segmentId, docCount] : segmentId2DocCount) {
        totalSize += indexlib::util::Bitmap::GetDumpSize(docCount);
    }
    return totalSize;
}

Status DeletionMapPatchWriter::GetPatchFileInfos(PatchFileInfos* patchFileInfos)
{
    assert(_workDir);
    for (const auto& [targetSegmentId, _] : _segmentId2Bitmaps) {
        PatchFileInfo patchFileInfo(_srcSegmentId, targetSegmentId, _workDir,
                                    DeletionMapUtil::GetDeletionMapFileName(targetSegmentId));
        patchFileInfos->PushBack(patchFileInfo);
    }
    return Status::OK();
}

const std::map<segmentid_t, std::shared_ptr<indexlib::util::ExpandableBitmap>>&
DeletionMapPatchWriter::GetSegmentId2Bitmaps() const
{
    return _segmentId2Bitmaps;
}

} // namespace indexlibv2::index
