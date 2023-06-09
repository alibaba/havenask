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
#ifndef __INDEXLIB_KV_MERGE_WRITER_H
#define __INDEXLIB_KV_MERGE_WRITER_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index, KVFormatOptions);
namespace autil { namespace mem_pool {
class PoolBase;
}} // namespace autil::mem_pool

namespace indexlib { namespace index {

class KVMergeWriter
{
public:
    KVMergeWriter() {}
    virtual ~KVMergeWriter() {}

public:
    virtual void Init(const file_system::DirectoryPtr& segmentDirectory,
                      const file_system::DirectoryPtr& indexDirectory, const config::IndexPartitionSchemaPtr& mSchema,
                      const config::KVIndexConfigPtr& kvIndexConfig, const KVFormatOptionsPtr& kvOptions,
                      bool needStorePKeyValue, autil::mem_pool::PoolBase* pool,
                      const framework::SegmentMetricsVec& segmentMetrics,
                      const index_base::SegmentTopologyInfo& targetTopoInfo) = 0;

    virtual bool AddIfNotExist(const keytype_t& key, const autil::StringView& pkeyRawValue,
                               const autil::StringView& value, uint32_t timestamp, bool isDeleted,
                               regionid_t regionId) = 0;

    virtual void OptimizeDump() = 0;
    virtual void FillSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                    const std::string& groupName) = 0;

    virtual size_t EstimateMemoryUse(const config::KVIndexConfigPtr& kvIndexConfig, const KVFormatOptionsPtr& kvOptions,
                                     const framework::SegmentMetricsVec& segmentMetrics,
                                     const index_base::SegmentTopologyInfo& targetTopoInfo) const = 0;

protected:
    config::IndexPartitionSchemaPtr mSchema;
    config::KVIndexConfigPtr mKVConfig;
    file_system::DirectoryPtr mSegmentDirectory;
    file_system::DirectoryPtr mIndexDirectory;
    file_system::DirectoryPtr mKVDir;

    autil::mem_pool::Pool mPool;
    docid_t mCurrentDocId = 0;
    bool mNeedStorePKeyValue = false;
    index::AttributeWriterPtr mAttributeWriter;
    common::AttributeConvertorPtr mPkeyConvertor;
    std::map<index::keytype_t, autil::StringView> mHashKeyValueMap;
    std::unique_ptr<common::HashTableFileIterator> mHashTableFileIterator;
};

DEFINE_SHARED_PTR(KVMergeWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_KV_MERGE_WRITER_H
