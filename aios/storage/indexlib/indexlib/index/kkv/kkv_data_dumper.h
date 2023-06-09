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
#ifndef __INDEXLIB_KKV_DATA_DUMPER_H
#define __INDEXLIB_KKV_DATA_DUMPER_H

#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/dumpable_pkey_offset_iterator.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_traits.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename SKeyType>
class KKVDataDumper
{
public:
    KKVDataDumper(const config::IndexPartitionSchemaPtr& schema, const config::KKVIndexConfigPtr& kkvConfig,
                  bool storeTs)
        : mSchema(schema)
        , mKKVConfig(kkvConfig)
        , mStoreTs(storeTs)
    {
    }
    virtual ~KKVDataDumper() {}

public:
    virtual DumpablePKeyOffsetIteratorPtr GetPKeyOffsetIterator() = 0;
    virtual void ResetBufferParam(size_t writerBufferSize, bool useAsyncWriter) = 0;
    virtual void Reserve(size_t totalSkeyCount, size_t totalValueLen) = 0;
    virtual void Close() = 0;

    virtual size_t GetSKeyCount() const = 0;
    virtual size_t GetMaxSKeyCount() const = 0;
    virtual size_t GetMaxValueLen() const = 0;
    virtual void Collect(uint64_t pkey, SKeyType skey, uint32_t ts, uint32_t expireTime, bool isDeletedPkey,
                         bool isDeletedSkey, bool isLastNode, const autil::StringView& value, regionid_t regionId) = 0;

protected:
    const config::IndexPartitionSchemaPtr& mSchema;
    const config::KKVIndexConfigPtr& mKKVConfig;
    bool mStoreTs;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_DATA_DUMPER_H
