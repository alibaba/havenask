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
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/SegmentStatistics.h"

namespace indexlib::file_system {
struct ReaderOption;
class IDirectory;
} // namespace indexlib::file_system

namespace indexlibv2::framework {

class SegmentInfo : public autil::legacy::Jsonizable
{
public:
    static const uint32_t INVALID_SHARDING_ID = std::numeric_limits<uint32_t>::max();
    static const uint32_t INVALID_SHARDING_COUNT = std::numeric_limits<uint32_t>::max();

public:
    SegmentInfo();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    // std::pair<Status, std::string> ToString(bool compactFormat) const noexcept
    // Status FromString(const std::string& str) noexcept;

    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                const indexlib::file_system::ReaderOption& readerOption) noexcept;
    Status Load(const std::string& filePath) noexcept;
    Status Store(const std::shared_ptr<indexlib::file_system::IDirectory>& directory) const noexcept;

public:
    // whether has multi sharding data or not in current segment
    bool HasMultiSharding() const;
    bool GetDescription(const std::string& key, std::string& value) const;

    const std::map<std::string, std::string>& GetDescriptions() const;
    Locator GetLocator() const;
    uint64_t GetDocCount() const;
    int64_t GetTimestamp() const;
    uint32_t GetMaxTTL() const;
    uint32_t GetShardCount() const;
    uint32_t GetShardId() const;
    schemaid_t GetSchemaId() const;
    bool IsMergedSegment() const;
    std::pair<Status, SegmentStatistics> GetSegmentStatistics() const;

public:
    void AddDescription(const std::string& key, const std::string& value);
    void SetTimestamp(int64_t timestamp);
    void SetLocator(const Locator& locator);
    void SetShardId(uint32_t shardId);
    void SetShardCount(uint32_t shardCount);
    void SetMergedSegment(bool isMergedSegment);
    void SetSegmentStatistics(const SegmentStatistics& segStats);

    // TODO(zhuanghaolin.zhl) move to protected
public:
    std::map<std::string, std::string> descriptions;
    volatile uint64_t docCount = 0;
    int64_t timestamp = INVALID_TIMESTAMP;
    uint32_t maxTTL = 0;
    uint32_t shardCount = 1;
    uint32_t shardId = INVALID_SHARDING_ID;
    schemaid_t schemaId = DEFAULT_SCHEMAID;
    bool mergedSegment = false;

protected:
    ThreadSafeLocatorGuard locatorGuard;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
