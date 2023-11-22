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

#include "indexlib/codegen/codegen_object.h"
#include "indexlib/common_define.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_index_options.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);

namespace indexlib { namespace index {

class KVSegmentReaderImplBase : public codegen::CodegenObject
{
public:
    KVSegmentReaderImplBase() : mSegmentId(INVALID_SEGMENTID) {}

    virtual ~KVSegmentReaderImplBase() {}

public:
    virtual void Open(const config::KVIndexConfigPtr& kvIndexConfig, const index_base::SegmentData& segmentData) = 0;

    virtual FL_LAZY(bool)
        Get(const KVIndexOptions* options, keytype_t key, autil::StringView& value, uint64_t& ts, bool& isDeleted,
            autil::mem_pool::Pool* pool = NULL, KVMetricsCollector* collector = nullptr) const = 0;

public:
    segmentid_t GetSegmentId() const { return mSegmentId; }

protected:
    segmentid_t mSegmentId;
};

DEFINE_SHARED_PTR(KVSegmentReaderImplBase);
}} // namespace indexlib::index
