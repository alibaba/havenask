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
#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"

namespace indexlibv2::index {

class SingleValueAttributePatchFormatter
{
public:
    SingleValueAttributePatchFormatter() : _patchItemCount(0), _fileLength(0) {}
    ~SingleValueAttributePatchFormatter() {}

public:
    void InitForWrite(bool supportNull, const std::shared_ptr<indexlib::file_system::FileWriter>& output);
    Status InitForRead(bool supportNull, const std::shared_ptr<indexlib::file_system::FileReader>& input,
                       int64_t recordSize, int64_t fileLength);
    uint32_t GetPatchItemCount() const { return _patchItemCount; }
    Status Close();

    inline static void EncodedDocId(docid_t& docId) { docId = ~docId; }

    inline static docid_t CompareDocId(const docid_t& left, const docid_t& right)
    {
        if (left < 0 && right < 0) {
            return ~left < ~right;
        }
        if (right < 0) {
            return left < ~right;
        }
        if (left < 0) {
            return ~left < right;
        }
        return left < right;
    }

    inline static int32_t TailLength() { return sizeof(uint32_t); }

    inline Status Write(docid_t docId, uint8_t* value, uint8_t length)
    {
        auto status = _writer->Write(&docId, sizeof(docid_t)).Status();
        RETURN_IF_STATUS_ERROR(status, "writer docid failed.");
        if (docId >= 0) // only write not null value
        {
            status = _writer->Write(value, length).Status();
            RETURN_IF_STATUS_ERROR(status, "write value failed.");
        }
        _patchItemCount++;
        return Status::OK();
    }

    template <typename T>
    inline std::pair<Status, bool> Read(docid_t& docId, T& value, bool& isNull)
    {
        if (!HasNext()) {
            return std::make_pair(Status::OK(), false);
        }
        auto [st, readLen] = _reader->Read((void*)(&docId), sizeof(docid_t), _cursor).StatusWith();
        if (unlikely(!st.IsOK())) {
            AUTIL_LOG(ERROR, "read patch file failed, file path is: %s", _reader->DebugString().c_str());
            return std::make_pair(st, false);
        }
        if (readLen < (ssize_t)sizeof(docid_t)) {
            AUTIL_LOG(ERROR, "read patch file failed, file path is: %s", _reader->DebugString().c_str());
            return std::make_pair(Status::InternalError("read patch file failed"), false);
        }

        _cursor += sizeof(docid_t);
        if (_isSupportNull && docId < 0) {
            docId = ~docId;
            isNull = true;
            return std::make_pair(Status::OK(), true);
        }

        isNull = false;
        std::tie(st, readLen) = _reader->Read((void*)(&value), sizeof(T), _cursor).StatusWith();
        if (unlikely(!st.IsOK())) {
            AUTIL_LOG(ERROR, "read patch value failed, file path is: %s", _reader->DebugString().c_str());
            return std::make_pair(st, false);
        }
        if (readLen < (ssize_t)sizeof(T)) {
            AUTIL_LOG(ERROR, "read patch value failed, file path is: %s", _reader->DebugString().c_str());
            return std::make_pair(Status::InternalError("read patch value failed"), false);
        }

        _cursor += sizeof(T);
        return std::make_pair(Status::OK(), true);
    }

    bool HasNext() const
    {
        if (_reader) {
            if (_isSupportNull) {
                return _cursor < _fileLength - sizeof(uint32_t);
            }
            return _cursor < _fileLength;
        }
        return false;
    }

private:
    bool _isSupportNull;
    std::shared_ptr<indexlib::file_system::FileWriter> _writer;
    std::shared_ptr<indexlib::file_system::FileReader> _reader;
    uint64_t _cursor;
    uint64_t _patchItemCount;
    uint64_t _fileLength;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::index
