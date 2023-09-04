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

#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataReader.h"

namespace indexlibv2::index::ann {

class ParallelMergeSegmentDataReader final : public SegmentDataReader
{
public:
    ParallelMergeSegmentDataReader() = default;
    ~ParallelMergeSegmentDataReader() = default;

public:
    bool Init(const indexlib::file_system::DirectoryPtr& directory, bool isOnline) override;
    IndexDataReaderPtr GetIndexDataReader(index_id_t id) override;
    indexlib::file_system::FileReaderPtr GetPrimaryKeyReader() const override;
    indexlib::file_system::FileReaderPtr GetEmbeddingDataReader() const override;
    bool Close() override;
    bool IsClosed() const override { return _isClosed; }

public:
    static size_t EstimateOpenMemoryUse(const indexlib::file_system::DirectoryPtr& directory);

private:
    std::vector<SegmentDataReaderPtr> _segmentDataReaders;
    indexlib::file_system::FileReaderPtr _primaryKeyReader;
    indexlib::file_system::FileReaderPtr _embeddingDataReader;
    bool _isClosed;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<ParallelMergeSegmentDataReader> ParallelMergeSegmentDataReaderPtr;

} // namespace indexlibv2::index::ann
