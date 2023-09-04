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
    friend IndexDataReader;

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

private:
    size_t ReadIndexFile(void** buf, const IndexDataAddr& meta, size_t len, size_t offset) const;
    size_t GetIndexFileLength() const { return _indexFileReader->GetLength(); }
    void* GetIndexFileAddress() const { return _indexFileReader->GetBaseAddress(); }

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
    IndexDataReader(const SegmentDataReader* reader, const IndexDataAddr& meta)
        : _segmentDataReader(reader)
        , _indexDataAddress(meta)
    {
    }
    ~IndexDataReader() = default;

public:
    size_t Read(void** buf, size_t len, size_t offset)
    {
        return _segmentDataReader->ReadIndexFile(buf, _indexDataAddress, len, offset);
    }
    size_t GetLength() const { return _indexDataAddress.length; }
    void* GetIndexFileAddress() const
    {
        return (int8_t*)_segmentDataReader->GetIndexFileAddress() + _indexDataAddress.offset;
    }
    bool Close()
    {
        _segmentDataReader = nullptr;
        return true;
    }

private:
    const SegmentDataReader* _segmentDataReader {nullptr};
    const IndexDataAddr _indexDataAddress {};

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann
