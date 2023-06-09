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

#include "autil/StringView.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "indexlib/util/Status.h"

namespace autil {
class TimeoutTerminator;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::index {

class IKVIterator;

class IKVSegmentReader
{
public:
    virtual ~IKVSegmentReader() = default;

public:
    virtual FL_LAZY(indexlib::util::Status)
        Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool,
            KVMetricsCollector* collector, autil::TimeoutTerminator* timeoutTerminator) const = 0;
    virtual std::unique_ptr<IKVIterator> CreateIterator() = 0;
    virtual size_t EvaluateCurrentMemUsed() = 0;
};

} // namespace indexlibv2::index
