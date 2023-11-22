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
#include "indexlib/base/Types.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/util/Bitmap.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::util {
class MMapAllocator;
}
namespace indexlib::file_system {
class FileReader;
class IDirectory;
} // namespace indexlib::file_system

namespace indexlibv2::index {
struct DeletionMapFileHeader;
class DeletionMapMetrics;

class DeletionMapDiskIndexer : public IDiskIndexer
{
public:
    DeletionMapDiskIndexer(size_t docCount, segmentid_t segmentId);
    ~DeletionMapDiskIndexer();

    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

    bool IsDeleted(docid_t docid) const;
    Status Delete(docid_t docid);
    uint32_t GetDeletedDocCount() const;
    segmentid_t GetSegmentId() const;
    void TEST_InitWithoutOpen();

    std::pair<Status, std::unique_ptr<indexlib::util::Bitmap>> GetDeletionMapPatch(segmentid_t segmentId);
    Status ApplyDeletionMapPatch(indexlib::util::Bitmap* bitmap);
    void RegisterMetrics(const std::shared_ptr<DeletionMapMetrics>& metrics);
    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory);

private:
    Status LoadFileHeader(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader,
                          DeletionMapFileHeader& fileHeader);

private:
    size_t _docCount;
    segmentid_t _segmentId;
    std::shared_ptr<DeletionMapMetrics> _metrics;
    std::unique_ptr<indexlib::util::Bitmap> _bitmap;
    std::unique_ptr<indexlib::util::MMapAllocator> _allocator;
    std::unique_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<indexlib::file_system::IDirectory> _directory;

private:
    AUTIL_LOG_DECLARE();
};

inline bool DeletionMapDiskIndexer::IsDeleted(docid_t docid) const
{
    assert(docid < _docCount);
    return _bitmap->Test(docid);
}

} // namespace indexlibv2::index
