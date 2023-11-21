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

#include <memory>
#include <mutex>
#include <unordered_map>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"
#include "indexlib/index/inverted_index/patch/IInvertedIndexSegmentUpdater.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::index {
class PatchFileInfo;
}
namespace indexlib::file_system {
class IDirectory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlib { namespace index {

class InvertedIndexSegmentUpdater : public IInvertedIndexSegmentUpdater
{
private:
    using VectorAllocator = autil::mem_pool::pool_allocator<ComplexDocId>;
    using DocVector = std::vector<ComplexDocId, VectorAllocator>;
    using MapAllocator = autil::mem_pool::pool_allocator<std::pair<const uint64_t, DocVector>>;
    using HashMap =
        std::unordered_map<const uint64_t, DocVector, std::hash<uint64_t>, std::equal_to<uint64_t>, MapAllocator>;

public:
    InvertedIndexSegmentUpdater(segmentid_t segId,
                                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);
    ~InvertedIndexSegmentUpdater() = default;

public:
    void Update(docid_t docId, const indexlib::document::ModifiedTokens& modifiedTokens) override;
    void Update(docid_t localDocId, DictKeyInfo termKey, bool isDelete) override;
    Status Dump(const std::shared_ptr<file_system::IDirectory>& indexesDir, segmentid_t srcSegment) override;

    std::pair<Status, std::shared_ptr<indexlibv2::index::PatchFileInfo>>
    DumpToIndexDir(const std::shared_ptr<indexlib::file_system::IDirectory>& indexDir, segmentid_t srcSegment,
                   const std::vector<docid_t>* old2NewDocId);

private:
    static void RewriteDocList(const std::vector<docid_t>* old2NewDocId, DocVector& docList);
    static std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory, const std::string& fileName,
                     bool hasPatchCompress);

private:
    std::mutex _dataMutex;
    indexlib::util::SimplePool _simplePool;
    HashMap _hashMap;
    DocVector _nullTermDocs;
    size_t _docCount;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::index
