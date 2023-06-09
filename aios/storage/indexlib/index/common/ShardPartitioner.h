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
#include "indexlib/base/Status.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/KeyHasherTyped.h"

namespace indexlibv2::config {
class TabletSchema;
}

namespace indexlibv2::index {

class ShardPartitioner
{
public:
    ShardPartitioner();
    virtual ~ShardPartitioner();

public:
    Status Init(const std::shared_ptr<config::TabletSchema>& schema, uint32_t shardCount);
    Status Init(indexlib::HashFunctionType keyHasherType, uint32_t shardCount);
    Status Init(uint32_t shardCount);
    bool GetShardIdx(const autil::StringView& key, uint32_t& shardIdx)
    {
        assert(_keyHasherType != indexlib::hft_unknown);
        indexlib::dictkey_t hashValue;
        if (!indexlib::util::GetHashKey(_keyHasherType, key, hashValue)) {
            AUTIL_LOG(WARN, "get hash key[%s] failed", key.data());
            ERROR_COLLECTOR_LOG(WARN, "get hash key[%s] failed", key.data());
            return false;
        }
        shardIdx = hashValue & _shardMask;
        return true;
    }
    uint32_t GetShardCount() { return _shardCount; }

    void GetShardIdx(uint64_t keyHash, uint32_t& shardIdx);

private:
    virtual void InitKeyHasherType(const std::shared_ptr<config::TabletSchema>& schema) {};

protected:
    indexlib::HashFunctionType _keyHasherType = indexlib::hft_unknown;
    uint64_t _shardMask = 0;
    bool _isMultiRegion = false;
    uint32_t _shardCount = 1;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
