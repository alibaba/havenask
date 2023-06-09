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
#include "indexlib/index/attribute/patch/SingleValueAttributePatchReader.h"

#include "indexlib/file_system/file/SnappyCompressFileReader.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, SinglePatchFile);

SinglePatchFile::SinglePatchFile(segmentid_t segmentId, bool patchCompressed, bool supportNull, int64_t recordSize)
    : _fileLength(0)
    , _docId(INVALID_DOCID)
    , _isNull(false)
    , _value(0)
    , _segmentId(segmentId)
    , _isCompress(patchCompressed)
    , _isSupportNull(supportNull)
    , _recordSize(recordSize)
{
}

Status SinglePatchFile::Open(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                             const std::string& fileName)
{
    auto [status, fileReader] =
        directory->CreateFileReader(fileName, indexlib::file_system::FSOT_BUFFERED).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create file reader failed for file [%s].", fileName.c_str());
    assert(fileReader);
    _formatter.reset(new SingleValueAttributePatchFormatter);
    if (!_isCompress) {
        _fileReader = fileReader;
        _fileLength = _fileReader->GetLength();
        return _formatter->InitForRead(_isSupportNull, _fileReader, _recordSize, _fileLength);
    }
    auto compressFileReader = std::make_shared<indexlib::file_system::SnappyCompressFileReader>();
    try {
        if (!compressFileReader->Init(fileReader)) {
            AUTIL_LOG(ERROR, "read compressed patch file failed, file path is: %s", fileReader->DebugString().c_str());
            return Status::InternalError("read compressed patch file failed");
        }
    } catch (...) {
        AUTIL_LOG(ERROR, "init compress file reader throw expception. ");
        return Status::InternalError("init compress file reader throw expception. ");
    }
    _fileReader = compressFileReader;
    _fileLength = compressFileReader->GetUncompressedFileLength();
    auto st = _formatter->InitForRead(_isSupportNull, _fileReader, _recordSize, _fileLength);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "init patch formatter failed.");
        return st;
    }
    return Status::OK();
}
} // namespace indexlibv2::index
