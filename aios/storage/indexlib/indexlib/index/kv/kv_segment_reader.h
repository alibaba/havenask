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

#include "indexlib/common_define.h"
#include "indexlib/index/kv/hash_table_compress_var_segment_reader.h"
#include "indexlib/index/kv/hash_table_fix_segment_reader.h"
#include "indexlib/index/kv/hash_table_var_segment_reader.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_index_options.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index/kv/kv_segment_reader_impl_base.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

namespace indexlib { namespace index {

class KVSegmentReader : public codegen::CodegenObject
{
public:
    KVSegmentReader() : mSegmentId(INVALID_SEGMENTID) {}

    ~KVSegmentReader() {}

public:
    void Open(const config::KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segmentData);

    FL_LAZY(bool)
    Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t& ts, bool& isDeleted,
        autil::mem_pool::Pool* pool = NULL, KVMetricsCollector* collector = nullptr) const;
    void TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id) override;

private:
    bool IsVarLenSegment(const config::KVIndexConfigPtr& kvConfig);
    bool doCollectAllCode() override;

public:
    segmentid_t GetSegmentId() const { return mSegmentId; }

private:
    typedef HashTableCompressVarSegmentReader HashTableCompressVarSegmentReaderType;
    typedef HashTableVarSegmentReader HashTableVarSegmentReaderType;
    typedef HashTableFixSegmentReader HashTableFixSegmentReaderType;
    typedef KVSegmentReaderImplBase KVSegmentReaderBase; // todo codegen
    typedef std::shared_ptr<KVSegmentReaderBase> ReaderPtr;

private:
    segmentid_t mSegmentId;
    ReaderPtr mReader;
    std::string mActualReaderType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVSegmentReader);
////////////////////////////////////////////////
inline FL_LAZY(bool) KVSegmentReader::Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value,
                                          uint64_t& ts, bool& isDeleted, autil::mem_pool::Pool* pool,
                                          KVMetricsCollector* collector) const
{
    FL_CORETURN FL_COAWAIT mReader->Get(options, key, value, ts, isDeleted, pool, collector);
}
}} // namespace indexlib::index
