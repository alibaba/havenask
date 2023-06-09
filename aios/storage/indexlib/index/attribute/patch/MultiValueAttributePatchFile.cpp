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
#include "indexlib/index/attribute/patch/MultiValueAttributePatchFile.h"

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/file/SnappyCompressFileReader.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiValueAttributePatchFile);
MultiValueAttributePatchFile::MultiValueAttributePatchFile(segmentid_t segmentId,
                                                           const std::shared_ptr<config::AttributeConfig>& attrConfig)
    : _fileLength(0)
    , _cursor(0)
    , _docId(INVALID_DOCID)
    , _segId(segmentId)
    , _patchItemCount(0)
    , _maxPatchLen(0)
    , _patchDataLen(0)
    , _fixedValueCount(attrConfig->GetFieldConfig()->GetFixedMultiValueCount())
    , _patchCompressed(attrConfig->GetCompressType().HasPatchCompress())
{
    if (_fixedValueCount != -1) {
        _patchDataLen = attrConfig->GetFixLenFieldSize();
    }
}

Status MultiValueAttributePatchFile::InitPatchFileReader(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                                         const std::string& fileName)
{
    auto fsResult = dir->CreateFileReader(fileName, indexlib::file_system::FSOT_BUFFERED);
    if (!fsResult.OK()) {
        AUTIL_LOG(ERROR, "create patch file reader failed, file[%s] error[%d]", fileName.c_str(), fsResult.Code());
        return fsResult.Status();
    }
    auto fileReader = fsResult.result;
    if (!_patchCompressed) {
        _fileReader = fileReader;
        _fileLength = _fileReader->GetLength();
        return Status::OK();
    }
    auto compressFileReader = std::make_shared<indexlib::file_system::SnappyCompressFileReader>();
    if (!compressFileReader->Init(fileReader)) {
        AUTIL_LOG(ERROR, "Read compressed patch file failed, file path is: %s", fileReader->DebugString().c_str());
        return Status::InternalError("read compressed patch file failed.");
    }
    _fileReader = compressFileReader;
    _fileLength = compressFileReader->GetUncompressedFileLength();
    return Status::OK();
}

Status MultiValueAttributePatchFile::Open(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                                          const std::string& fileName)
{
    auto st = InitPatchFileReader(dir, fileName);
    RETURN_IF_STATUS_ERROR(st, "init patch file reader failed.");
    if (_fileLength < ((int64_t)sizeof(uint32_t) * 2)) {
        AUTIL_LOG(ERROR, "incomplete patch file: %s", _fileReader->DebugString().c_str());
        return Status::InternalError("incomplete patch file.");
    }

    size_t beginPos = _fileLength - sizeof(uint32_t) * 2;
    auto fsResult = _fileReader->Read((void*)&_patchItemCount, sizeof(uint32_t), beginPos);
    if (!fsResult.OK()) {
        AUTIL_LOG(ERROR, "read patch item count failed, error[%d]", fsResult.Code());
        return fsResult.Status();
    }
    if (fsResult.result < (size_t)sizeof(uint32_t)) {
        AUTIL_LOG(ERROR, "invalid patch item count, file:%s", _fileReader->DebugString().c_str());
        return Status::InternalError("invalid patch item count.");
    }
    beginPos += sizeof(uint32_t);
    fsResult = _fileReader->Read((void*)&_maxPatchLen, sizeof(uint32_t), beginPos);
    if (!fsResult.OK()) {
        AUTIL_LOG(ERROR, "read max patch len failed, error[%d]", fsResult.Code());
        return fsResult.Status();
    }
    if (fsResult.result < (ssize_t)sizeof(uint32_t)) {
        AUTIL_LOG(ERROR, "invalid max patch len, file:%s", _fileReader->DebugString().c_str());
        return Status::InternalError("invalid max patch len.");
    }
    return Status::OK();
}
} // namespace indexlibv2::index
