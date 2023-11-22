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

#include "indexlib/common_define.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/index/kv/kv_typeid.h"

DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, PackAttributeConfig);
DECLARE_REFERENCE_CLASS(framework, SegmentGroupMetrics);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, KVSegmentIterator);
DECLARE_REFERENCE_CLASS(index, KVWriter);
DECLARE_REFERENCE_CLASS(index, KVMergeWriter);

namespace autil { namespace mem_pool {
class PoolBase;
}} // namespace autil::mem_pool

namespace indexlib { namespace index {

class KVFactory
{
public:
    static KVTypeId GetKVTypeId(const config::KVIndexConfigPtr& kvConfig, const KVFormatOptionsPtr& kvOptions);

    static KVWriterPtr CreateWriter(const config::KVIndexConfigPtr& kvIndexConfig);

    static KVMergeWriterPtr CreateMergeWriter(const config::KVIndexConfigPtr& kvIndexConfig);

private:
    static void FillIndexType(const config::KVIndexConfigPtr& kvConfig, KVTypeId& kvTypeId);

    static void FillCompactHashFormat(const config::KVIndexConfigPtr& kvConfig, const KVFormatOptionsPtr& kvOptions,
                                      bool isVarLen, KVTypeId& kvTypeId);

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
