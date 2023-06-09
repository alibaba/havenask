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
#include "indexlib/index/attribute/format/SingleValueAttributePatchFormatter.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, SingleValueAttributePatchFormatter);

void SingleValueAttributePatchFormatter::InitForWrite(bool supportNull,
                                                      const std::shared_ptr<indexlib::file_system::FileWriter>& writer)
{
    _isSupportNull = supportNull;
    _writer = writer;
}

Status SingleValueAttributePatchFormatter::InitForRead(bool supportNull,
                                                       const std::shared_ptr<indexlib::file_system::FileReader>& input,
                                                       int64_t recordSize, int64_t fileLength)
{
    _isSupportNull = supportNull;
    _reader = input;
    _cursor = 0;
    _fileLength = fileLength;
    if (!supportNull) {
        // not support null, calc item count by length
        _patchItemCount = _fileLength / (sizeof(docid_t) + recordSize);
        if (_patchItemCount * (sizeof(docid_t) + recordSize) != _fileLength) {
            AUTIL_LOG(ERROR, "patch[%s] file length[%lu] is inconsistent", _reader->DebugString().c_str(), _fileLength);
            return Status::InternalError("mismatch patch file len");
        }
        return Status::OK();
    }
    // read item count by tail
    size_t beginPos = fileLength - sizeof(uint32_t);
    try {
        if (_reader->Read((void*)&_patchItemCount, sizeof(uint32_t), beginPos).GetOrThrow() <
            (size_t)sizeof(uint32_t)) {
            AUTIL_LOG(ERROR, "Read patch item count from patch file[%s] failed", _reader->DebugString().c_str());
            return Status::InternalError("mismatch patch item count");
        }
    } catch (...) {
        AUTIL_LOG(ERROR, "patch read throw expception. ");
        return Status::InternalError("patch read throw expception. ");
    }
    return Status::OK();
}

Status SingleValueAttributePatchFormatter::Close()
{
    Status st;
    if (_writer) {
        if (_isSupportNull) {
            st = _writer->Write(&_patchItemCount, sizeof(uint32_t)).Status();
            if (!st.IsOK()) {
                AUTIL_LOG(ERROR, "write patch item count failed, itemCount[%lu]", _patchItemCount);
                return st;
            }
        }
        st = _writer->Close().Status();
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "close writer failed.");
            return st;
        }
    }
    if (_reader) {
        st = _reader->Close().Status();
        if (!st.IsOK()) {
            AUTIL_LOG(ERROR, "close writer failed.");
            return st;
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::index
