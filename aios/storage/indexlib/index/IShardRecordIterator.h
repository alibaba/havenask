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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {
class AdapterIgnoreFieldCalculator;
} // namespace indexlibv2::index

namespace indexlibv2::index {

class IShardRecordIterator : private autil::NoCopyable
{
public:
    struct ShardCheckpoint {
        offset_t first = 0;
        offset_t second = 0;
    };
    struct ShardRecord {
        typedef uint64_t keytype_t;
        keytype_t key = 0;
        autil::StringView value;
        uint32_t timestamp = 0;
        indexlib::util::KeyValueMap otherFields;
    };

    virtual ~IShardRecordIterator() = default;

public:
    virtual Status Init(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                        const std::map<std::string, std::string>& params,
                        const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                        const std::shared_ptr<index::AdapterIgnoreFieldCalculator>& ignoreFieldCalculator,
                        int64_t currentTs) = 0;
    virtual Status Next(ShardRecord* record, std::string* checkpoint) = 0;
    virtual bool HasNext() = 0;
    virtual Status Seek(const std::string& checkpoint) = 0;
};

} // namespace indexlibv2::index
