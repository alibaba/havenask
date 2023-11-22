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

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/IndexDataAddrHolder.h"

namespace indexlibv2::index::ann {

class IndexDataReader;
typedef std::shared_ptr<IndexDataReader> IndexDataReaderPtr;

class SegmentDataReader
{
public:
    SegmentDataReader() : _isClosed(false) {}
    virtual ~SegmentDataReader() = default;

public:
    virtual bool Init(const indexlib::file_system::DirectoryPtr& directory, bool isOnline = true);
    virtual IndexDataReaderPtr GetIndexDataReader(index_id_t id);
    virtual indexlib::file_system::FileReaderPtr GetPrimaryKeyReader() const;
    virtual indexlib::file_system::FileReaderPtr GetEmbeddingDataReader() const;
    virtual bool Close();
    virtual bool IsClosed() const { return _isClosed; }

public:
    static size_t EstimateOpenMemoryUse(const indexlib::file_system::DirectoryPtr& directory);

public:
    IndexDataAddrHolder TEST_GetIndexDataAddrHolder() { return _indexDataAddrHolder; }

private:
    indexlib::file_system::FileReaderPtr _indexFileReader;
    indexlib::file_system::FileReaderPtr _primaryKeyReader;
    indexlib::file_system::FileReaderPtr _embeddingDataReader;
    IndexDataAddrHolder _indexDataAddrHolder;

private:
    AUTIL_LOG_DECLARE();
    bool _isClosed;
};
typedef std::shared_ptr<SegmentDataReader> SegmentDataReaderPtr;

class IndexDataReader
{
public:
    IndexDataReader(const indexlib::file_system::FileReaderPtr& fileReader, const IndexDataAddr& addr)
        : _fileReader(fileReader)
        , _indexDataAddress(addr)
    {
        const char* fileAddress = reinterpret_cast<const char*>(_fileReader->GetBaseAddress());
        if (fileAddress != nullptr) {
            _baseAddress = fileAddress + _indexDataAddress.offset;
        }
    }

    IndexDataReader(const indexlib::file_system::FileReaderPtr& fileReader) : _fileReader(fileReader)
    {
        const char* fileAddress = reinterpret_cast<const char*>(_fileReader->GetBaseAddress());
        if (fileAddress != nullptr) {
            _baseAddress = fileAddress + _indexDataAddress.offset;
        }
        _indexDataAddress.offset = 0ul;
        _indexDataAddress.length = _fileReader->GetLength();
    }
    ~IndexDataReader() = default;

public:
    size_t Read(void* buf, size_t len, size_t offset)
    {
        size_t regionSize = _indexDataAddress.length;
        if (offset + len > regionSize) {
            if (offset > regionSize) {
                offset = regionSize;
            }
            len = regionSize - offset;
        }

        size_t fileOffset = _indexDataAddress.offset + offset;
        auto status = _fileReader->Read(buf, len, fileOffset);
        if (status.OK()) {
            return status.Value();
        }
        AUTIL_LOG(ERROR, "failed to read, len[%lu] offset[%lu] file[%s]", len, fileOffset,
                  _fileReader->DebugString().c_str());
        return 0;
    }

    size_t GetLength() const { return _indexDataAddress.length; }

    const char* GetBaseAddress() const { return _baseAddress; }

private:
    indexlib::file_system::FileReaderPtr _fileReader {nullptr};
    const char* _baseAddress {nullptr};
    IndexDataAddr _indexDataAddress {};

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
