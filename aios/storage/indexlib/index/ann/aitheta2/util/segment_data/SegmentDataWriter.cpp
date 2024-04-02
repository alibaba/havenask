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
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataWriter.h"

#include "autil/legacy/exception.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {

SegmentDataWriter::SegmentDataWriter() {}
SegmentDataWriter::~SegmentDataWriter()
{
    try {
        Close();
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "destruct failed, error[%s]", e.what());
    }
}

bool SegmentDataWriter::Init(const indexlib::file_system::DirectoryPtr& directory)
{
    try {
        _directory = directory;
        _directory->RemoveFile(INDEX_FILE, RemoveOption::MayNonExist());
        _indexFileWriter = _directory->CreateFileWriter(INDEX_FILE);
        return _indexFileWriter != nullptr;
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "init failed, error[%s]", e.what());
    }
    return false;
}

IndexDataWriterPtr SegmentDataWriter::GetIndexDataWriter(index_id_t indexId)
{
    assert(_indexFileWriter);
    if (_currentIndexDataWriter && !_currentIndexDataWriter->IsClosed()) {
        AUTIL_LOG(ERROR, "old index data writer not released, create new failed");
        return IndexDataWriterPtr();
    }
    if (_indexDataAddrHolder.HasAddr(indexId)) {
        AUTIL_LOG(ERROR, "indexId[%lu] exists, create failed", indexId);
        return IndexDataWriterPtr();
    }
    _currentIndexDataWriter = std::make_shared<IndexDataWriter>(_indexFileWriter, &_indexDataAddrHolder, indexId);
    return _currentIndexDataWriter;
}

FileWriterPtr SegmentDataWriter::GetPrimaryKeyWriter()
{
    if (!_primaryKeyWriter) {
        _directory->RemoveFile(PRIMARY_KEY_FILE, RemoveOption::MayNonExist());
        _primaryKeyWriter = _directory->CreateFileWriter(PRIMARY_KEY_FILE);
    }
    return _primaryKeyWriter;
}

FileWriterPtr SegmentDataWriter::GetEmbeddingDataWriter()
{
    if (!_embeddingDataWriter) {
        _directory->RemoveFile(EMBEDDING_DATA_FILE, RemoveOption::MayNonExist());
        _embeddingDataWriter = _directory->CreateFileWriter(EMBEDDING_DATA_FILE);
    }
    return _embeddingDataWriter;
}

bool SegmentDataWriter::Close()
{
    if (_isClosed) {
        return true;
    }
    ANN_CHECK(_currentIndexDataWriter == nullptr || _currentIndexDataWriter->IsClosed(),
              "current index data writer not closed");
    ANN_CHECK(_indexDataAddrHolder.Dump(_directory), "index data address dump failed");

    ANN_CHECK(_indexFileWriter->Close().OK(), "close failed");
    ANN_CHECK(_primaryKeyWriter == nullptr || _primaryKeyWriter->Close().OK(), "close failed");
    ANN_CHECK(_embeddingDataWriter == nullptr || _embeddingDataWriter->Close().OK(), "close failed");

    _isClosed = true;
    AUTIL_LOG(INFO, "close segment data writer success");
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, IndexDataWriter);
AUTIL_LOG_SETUP(indexlib.index, SegmentDataWriter);
} // namespace indexlibv2::index::ann
