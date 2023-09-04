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
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"

using namespace std;
using namespace indexlib::file_system;

static const float kLoadMemoryScalingFactor = 1.1f;

namespace indexlibv2::index::ann {

bool SegmentDataReader::Init(const DirectoryPtr& directory, bool isOnline)
{
    try {
        indexlib::file_system::ReaderOption readerOption(FSOT_MEM_ACCESS);
        readerOption.mayNonExist = true;
        _indexFileReader = directory->CreateFileReader(INDEX_FILE, readerOption);
        ANN_CHECK(_indexFileReader, "create online index file reader failed");
        ANN_CHECK(GetIndexFileLength() == 0 || GetIndexFileAddress() != 0, "get online index file base address failed");

        ANN_CHECK(_indexDataAddrHolder.Load(directory), "load failed");

        if (isOnline) {
            return true;
        }

        readerOption.openType = FSOT_BUFFERED;
        if (directory->IsExist(PRIMARY_KEY_FILE)) {
            _primaryKeyReader = directory->CreateFileReader(PRIMARY_KEY_FILE, readerOption);
            ANN_CHECK(_primaryKeyReader != nullptr, "create pk file reader failed");
        }
        if (directory->IsExist(EMBEDDING_DATA_FILE)) {
            _embeddingDataReader = directory->CreateFileReader(EMBEDDING_DATA_FILE, readerOption);
            ANN_CHECK(_embeddingDataReader != nullptr, "create embedding file reader failed");
        }
    } catch (const indexlib::util::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "open segment data reader, error[%s]", e.what());
        return false;
    }

    return true;
}

size_t SegmentDataReader::EstimateOpenMemoryUse(const DirectoryPtr& directory)
{
    return directory->EstimateFileMemoryUse(INDEX_FILE, FSOT_MEM_ACCESS) * kLoadMemoryScalingFactor;
}

FileReaderPtr SegmentDataReader::GetPrimaryKeyReader() const { return _primaryKeyReader; }

FileReaderPtr SegmentDataReader::GetEmbeddingDataReader() const { return _embeddingDataReader; }

bool SegmentDataReader::Close()
{
    if (_isClosed) {
        return true;
    }
    ANN_CHECK(_indexFileReader->Close().OK(), "close failed");
    ANN_CHECK(_primaryKeyReader == nullptr || _primaryKeyReader->Close().OK(), "close failed");
    ANN_CHECK(_embeddingDataReader == nullptr || _embeddingDataReader->Close().OK(), "close failed");
    _isClosed = true;
    return true;
}

IndexDataReaderPtr SegmentDataReader::GetIndexDataReader(index_id_t id)
{
    assert(!_isClosed);

    IndexDataAddr meta;
    if (_indexDataAddrHolder.GetAddr(id, meta)) {
        return IndexDataReaderPtr(new IndexDataReader(this, meta));
    }
    AUTIL_LOG(DEBUG, "get index data reader[%ld] failed", id);
    return IndexDataReaderPtr();
}

size_t SegmentDataReader::ReadIndexFile(void** buf, const IndexDataAddr& meta, size_t len, size_t off) const
{
    assert(!_isClosed);

    int8_t* base = (int8_t*)GetIndexFileAddress() + meta.offset;
    if (off + len > meta.length) {
        len = meta.length > off ? (meta.length - off) : 0ul;
    }
    *buf = base + off;
    return len;
}

AUTIL_LOG_SETUP(indexlib.index, IndexDataReader);
AUTIL_LOG_SETUP(indexlib.index, SegmentDataReader);
} // namespace indexlibv2::index::ann
