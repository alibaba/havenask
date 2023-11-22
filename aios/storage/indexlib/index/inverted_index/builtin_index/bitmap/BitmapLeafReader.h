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
#include "future_lite/coro/Lazy.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingExpandData.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::file_system {
class FileReader;
struct ReadOption;
class ResourceFile;
} // namespace indexlib::file_system

namespace indexlib::index {
class DictKeyInfo;
class DictionaryReader;
class InvertedIndexSearchTracer;
class SegmentPosting;

struct ExpandDataTable {
    autil::mem_pool::UnsafePool pool;
    util::HashMap<dictkey_t, BitmapPostingExpandData*, autil::mem_pool::UnsafePool> table;
    BitmapPostingExpandData* nullTermData;

    ExpandDataTable() : table(&pool, HASHMAP_INIT_SIZE), nullTermData(nullptr) {}
    ExpandDataTable(const ExpandDataTable&) = delete;
    ExpandDataTable& operator=(const ExpandDataTable&) = delete;
    ~ExpandDataTable()
    {
        auto iter = table.CreateIterator();
        while (iter.HasNext()) {
            auto kvpair = iter.Next();
            kvpair.second->postingWriter->~BitmapPostingWriter();
        }
        table.Clear();
        if (nullTermData) {
            nullTermData->postingWriter->~BitmapPostingWriter();
        }
    }
};

class BitmapLeafReader
{
public:
    BitmapLeafReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                     const std::shared_ptr<DictionaryReader>& dictReader,
                     const std::shared_ptr<file_system::FileReader>& postingReader,
                     const std::shared_ptr<file_system::ResourceFile>& expandResource, uint64_t docCount);
    ~BitmapLeafReader() = default;

    Result<bool> GetSegmentPosting(const DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                                   file_system::ReadOption option, InvertedIndexSearchTracer* tracer) const noexcept;
    void Update(docid_t docId, const DictKeyInfo& key, bool isDelete);

    std::shared_ptr<file_system::ResourceFile> GetExpandResource() const { return _expandResource; }
    uint64_t GetDocCount() const { return _docCount; }
    size_t EvaluateCurrentMemUsed() const;

private:
    static bool TryUpdateInOriginalBitmap(uint8_t* segmentPostingBaseAddr, docid64_t docId, bool isDelete,
                                          BitmapPostingExpandData* expandData);
    static BitmapPostingExpandData* CreateExpandData(ExpandDataTable* dataTable, DictionaryReader* dictReader,
                                                     file_system::FileReader* postingReader,
                                                     const DictKeyInfo& key) noexcept;

private:
    Result<bool> GetSegmentPostingFromIndex(const DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                                            file_system::ReadOption option,
                                            InvertedIndexSearchTracer* tracer) const noexcept;

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::shared_ptr<DictionaryReader> _dictReader;
    std::shared_ptr<file_system::FileReader> _postingReader;
    std::shared_ptr<file_system::ResourceFile> _expandResource;
    IndexFormatOption _indexFormatOption;
    uint64_t _docCount = 0;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
