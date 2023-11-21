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
#include <unordered_set>

#include "autil/EnvUtil.h"
#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/kkv/building_kkv_segment_reader.h"
#include "indexlib/index/kkv/built_kkv_segment_reader.h"
#include "indexlib/index/kkv/cached_kkv_iterator_impl.h"
#include "indexlib/index/kkv/kkv_coro_define.h"
#include "indexlib/index/kkv/kkv_index_options.h"
#include "indexlib/index/kkv/kkv_iterator.h"
#include "indexlib/index/kkv/kkv_search_coroutine.h"
#include "indexlib/index/kkv/on_disk_kkv_iterator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ColumnUtil.h"
#include "indexlib/util/KeyHasherTyped.h"
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

namespace indexlib { namespace index {

class KKVReaderImplBase : public codegen::CodegenObject
{
public:
    KKVReaderImplBase(const std::string& schemaName) : mSKeyDictFieldType(ft_unknown), mSchemaName(schemaName) {}

    virtual ~KKVReaderImplBase() {}

public:
    virtual void Open(const config::KKVIndexConfigPtr& kkvConfig, const index_base::PartitionDataPtr& partitionData,
                      const util::SearchCachePartitionWrapperPtr& searchCache) = 0;
    virtual FL_LAZY(KKVIterator*)
        Lookup(KKVIndexOptions* indexOptions, PKeyType pkeyHash, std::vector<uint64_t> suffixKeyHashVec,
               uint64_t timestamp, TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
               KVMetricsCollector* metricsCollector = nullptr) = 0;

    virtual FL_LAZY(KKVIterator*)
        Lookup(KKVIndexOptions* indexOptions, PKeyType pkeyHash,
               std::vector<uint64_t, autil::mem_pool::pool_allocator<uint64_t>> suffixKeyHashVec, uint64_t timestamp,
               TableSearchCacheType searchCacheType, autil::mem_pool::Pool* pool,
               KVMetricsCollector* metricsCollector = nullptr) = 0;

    FieldType GetSKeyDictFieldType() const { return mSKeyDictFieldType; }

protected:
    FieldType mSKeyDictFieldType;
    std::string mSchemaName;
};

DEFINE_SHARED_PTR(KKVReaderImplBase);
}} // namespace indexlib::index
