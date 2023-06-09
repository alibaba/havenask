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
#ifndef __INDEXLIB_KV_DOC_TIMESTAMP_DECIDER_H
#define __INDEXLIB_KV_DOC_TIMESTAMP_DECIDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KvDocTimestampDecider
{
public:
    KvDocTimestampDecider();
    ~KvDocTimestampDecider();

public:
    void Init(const config::IndexPartitionOptions& option);

    uint32_t GetTimestamp(document::KVIndexDocument* doc);

private:
    config::IndexPartitionOptions mOption;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KvDocTimestampDecider);
}} // namespace indexlib::index

#endif //__INDEXLIB_KV_DOC_TIMESTAMP_DECIDER_H
