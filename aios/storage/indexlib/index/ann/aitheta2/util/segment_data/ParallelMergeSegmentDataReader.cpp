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
#include "indexlib/index/ann/aitheta2/util/segment_data/ParallelMergeSegmentDataReader.h"

#include "indexlib/index/ann/aitheta2/util/parallel_merge/ParallelMergeUtil.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {

bool ParallelMergeSegmentDataReader::Init(const indexlib::file_system::DirectoryPtr& directory, bool isOnline)
{
    vector<indexlib::file_system::DirectoryPtr> subDirs;

    ParallelReduceMeta meta;
    ANN_CHECK(meta.Load(directory), "load meta failed");
    ParallelMergeUtil::GetParallelMergeDirs(directory, meta, subDirs);

    for (auto& subDirectory : subDirs) {
        auto reader = make_shared<SegmentDataReader>();
        ANN_CHECK(reader->Init(subDirectory, isOnline), "open reader failed");
        _segmentDataReaders.push_back(reader);
    }
    if (isOnline) {
        return true;
    }

    indexlib::file_system::ReaderOption bufferedOption(FSOT_BUFFERED);
    bufferedOption.mayNonExist = true;
    if (directory->IsExist(PRIMARY_KEY_FILE)) {
        _primaryKeyReader = directory->CreateFileReader(PRIMARY_KEY_FILE, bufferedOption);
        ANN_CHECK(_primaryKeyReader, "create primary key reader failed");
    }
    if (directory->IsExist(EMBEDDING_DATA_FILE)) {
        _embeddingDataReader = directory->CreateFileReader(EMBEDDING_DATA_FILE, bufferedOption);
        ANN_CHECK(_embeddingDataReader, "create embedding data reader failed");
    }

    return true;
}

size_t ParallelMergeSegmentDataReader::EstimateOpenMemoryUse(const indexlib::file_system::DirectoryPtr& directory)
{
    vector<indexlib::file_system::DirectoryPtr> subDirs;

    ParallelReduceMeta meta;
    meta.Load(directory);
    ParallelMergeUtil::GetParallelMergeDirs(directory, meta, subDirs);

    size_t memory = 0ul;
    for (auto& subDirectory : subDirs) {
        memory += SegmentDataReader::EstimateOpenMemoryUse(subDirectory);
    }
    return memory;
}

IndexDataReaderPtr ParallelMergeSegmentDataReader::GetIndexDataReader(index_id_t id)
{
    for (auto& segDataReader : _segmentDataReaders) {
        auto reader = segDataReader->GetIndexDataReader(id);
        if (reader) {
            return reader;
        }
    }
    AUTIL_LOG(DEBUG, "get index data reader[%ld] failed", id);
    return IndexDataReaderPtr();
}

bool ParallelMergeSegmentDataReader::Close()
{
    if (_isClosed) {
        return true;
    }
    for (auto& segDataReader : _segmentDataReaders) {
        ANN_CHECK(segDataReader->Close(), "close failed");
    }
    ANN_CHECK(_primaryKeyReader == nullptr || _primaryKeyReader->Close().OK(), "close failed");
    ANN_CHECK(_embeddingDataReader == nullptr || _embeddingDataReader->Close().OK(), "close failed");
    _isClosed = true;
    return true;
}

FileReaderPtr ParallelMergeSegmentDataReader::GetPrimaryKeyReader() const { return _primaryKeyReader; }

FileReaderPtr ParallelMergeSegmentDataReader::GetEmbeddingDataReader() const { return _embeddingDataReader; }

AUTIL_LOG_SETUP(indexlib.index, ParallelMergeSegmentDataReader);
} // namespace indexlibv2::index::ann
