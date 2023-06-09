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
#include "indexlib/index/inverted_index/patch/IndexPatchFileReader.h"

#include "indexlib/file_system/file/SnappyCompressFileReader.h"
#include "indexlib/index/inverted_index/format/PatchFormat.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, IndexPatchFileReader);

IndexPatchFileReader::IndexPatchFileReader(segmentid_t srcSegmentId, segmentid_t targetSegmentId, bool patchCompress)
    : _dataLength(0)
    , _srcSegmentId(srcSegmentId)
    , _targetSegmentId(targetSegmentId)
    , _patchCompressed(patchCompress)
    , _cursor(-1)
    , _curLeftDocs(0)
    , _leftNonNullTermCount(0)
{
}

Status IndexPatchFileReader::Open(const std::shared_ptr<file_system::IDirectory>& directory,
                                  const std::string& fileName)
{
    Status status = Status::OK();
    std::tie(status, _fileReader) =
        directory->CreateFileReader(fileName, file_system::ReaderOption::NoCache(file_system::FSOT_BUFFERED))
            .StatusWith();
    RETURN_IF_STATUS_ERROR(status, "Read patch file failed, file path is: %s", fileName.c_str());

    if (_patchCompressed) {
        file_system::SnappyCompressFileReaderPtr compressFileReader(new file_system::SnappyCompressFileReader);

        if (!compressFileReader->Init(_fileReader)) {
            AUTIL_LOG(WARN, "Read compressed patch file failed, file path is: %s", _fileReader->DebugString().c_str());
            return Status::Corruption("Read compressed patch file failed, file path is: %s",
                                      _fileReader->DebugString().c_str());
        }
        _fileReader = compressFileReader;
        _dataLength = compressFileReader->GetUncompressedFileLength();
    } else {
        _dataLength = _fileReader->GetLength();
    }
    assert(_dataLength >= sizeof(_meta));

    _dataLength -= sizeof(_meta);
    _fileReader->Read(&_meta, sizeof(_meta), _dataLength).GetOrThrow();
    _leftNonNullTermCount = _meta.nonNullTermCount;

    _cursor = 0;
    size_t totalTermCount = _leftNonNullTermCount;
    if (_meta.hasNullTerm) {
        totalTermCount += 1;
    }
    size_t metaSize = (sizeof(/*termKey*/ uint64_t) + sizeof(/*docCount*/ uint32_t)) * totalTermCount;
    _patchItemCount = (_dataLength - metaSize) / sizeof(ComplexDocId);
    return Status::OK();
}

bool IndexPatchFileReader::HasNext() const
{
    assert(_fileReader);
    return _cursor < _dataLength;
}

void IndexPatchFileReader::Next()
{
    assert(_fileReader);
    assert(_cursor < _dataLength);
    if (_curLeftDocs == 0) {
        uint64_t dictKey;
        _fileReader->Read(&dictKey, sizeof(dictKey), _cursor).GetOrThrow();
        if (_leftNonNullTermCount == 0) {
            assert(_meta.hasNullTerm);
            _currentTermKey = index::DictKeyInfo(dictKey, /*isNull*/ true);
        } else {
            _currentTermKey = index::DictKeyInfo(dictKey, /*isNull*/ false);
            --_leftNonNullTermCount;
        }
        _cursor += sizeof(dictKey);
        _fileReader->Read(&_curLeftDocs, sizeof(_curLeftDocs), _cursor).GetOrThrow();
        _cursor += sizeof(_curLeftDocs);
        assert(_curLeftDocs != 0);
    }
    _fileReader->Read(&_currentDocId, sizeof(_currentDocId), _cursor).GetOrThrow();
    _cursor += sizeof(_currentDocId);
    --_curLeftDocs;
}

void IndexPatchFileReader::SkipCurrentTerm()
{
    assert(_fileReader);
    assert(_cursor <= _dataLength);
    _cursor += _curLeftDocs * sizeof(_currentDocId);
    _curLeftDocs = 0;
}
} // namespace indexlib::index
