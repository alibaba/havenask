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
#include "indexlib/framework/SegmentInfo.h"

#include <cstdint>
#include <exception>

#include "autil/NetUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Timer.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, SegmentInfo);

SegmentInfo::SegmentInfo()
{
    descriptions[indexlib::SEGMENT_INIT_TIMESTAMP] =
        autil::StringUtil::toString(indexlib::util::Timer::GetCurrentTimeInMicroSeconds());
}

void SegmentInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    uint64_t tempDocCount = docCount;
    json.Jsonize("DocCount", tempDocCount, tempDocCount);
    docCount = tempDocCount;
    json.Jsonize("Timestamp", timestamp, timestamp);
    json.Jsonize("MaxTTL", maxTTL, maxTTL);
    json.Jsonize("IsMergedSegment", mergedSegment, mergedSegment);
    // TODO(xinfei.sxf) refactor name to remove column
    json.Jsonize("ShardingColumnCount", shardCount, shardCount);
    json.Jsonize("ShardingColumnId", shardId, shardId);
    json.Jsonize("Descriptions", descriptions, descriptions);

    if (json.GetMode() == TO_JSON) {
        auto curLocator = locatorGuard.Get();
        std::string locatorString = autil::StringUtil::strToHexStr(curLocator.Serialize());
        json.Jsonize("Locator", locatorString);
        if (schemaId != DEFAULT_SCHEMAID) {
            json.Jsonize("schemaVersionId", schemaId);
        }
        json.Jsonize("hostname", autil::NetUtil::getBindIp());
    } else {
        json.Jsonize("schemaVersionId", schemaId, schemaId);
        std::string locatorString;
        json.Jsonize("Locator", locatorString);
        locatorString = autil::StringUtil::hexStrToStr(locatorString);
        if (!locatorString.empty()) {
            Locator curLocator;
            if (!curLocator.Deserialize(locatorString)) {
                INDEXLIB_THROW(indexlib::util::RuntimeException, "deserialize locator string [%s] failed",
                               locatorString.c_str());
            } else {
                locatorGuard.Set(curLocator);
            }
        }
    }
}

Status SegmentInfo::Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                         const indexlib::file_system::ReaderOption& readerOption) noexcept
{
    std::string content;
    // auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(FSOT_MEM);
    auto st = directory->Load(SEGMENT_INFO_FILE_NAME, readerOption, content);
    if (!st.OK()) {
        return st.Status();
    }
    return indexlib::file_system::JsonUtil::FromString(content, this).Status();
}

Status SegmentInfo::Load(const std::string& filePath) noexcept
{
    std::string content;
    auto st = indexlib::file_system::FslibWrapper::AtomicLoad(filePath, content);
    if (!st.OK()) {
        return st.Status();
    }
    return indexlib::file_system::JsonUtil::FromString(content, this).Status();
}

Status SegmentInfo::Store(const std::shared_ptr<indexlib::file_system::IDirectory>& directory) const noexcept
{
    auto [status, content] = indexlib::file_system::JsonUtil::ToString<SegmentInfo>(*this).StatusWith();
    if (!status.IsOK()) {
        return status;
    }
    indexlib::file_system::WriterOption writerOption;
    writerOption.atomicDump = true;
    writerOption.notInPackage = true;
    return directory->Store(SEGMENT_INFO_FILE_NAME, content, writerOption).Status();
}

void SegmentInfo::AddDescription(const std::string& key, const std::string& value) { descriptions[key] = value; }

bool SegmentInfo::GetDescription(const std::string& key, std::string& value) const
{
    auto iter = descriptions.find(key);
    if (iter == descriptions.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

bool SegmentInfo::HasMultiSharding() const { return shardCount > 1 && shardId == INVALID_SHARDING_ID; }

const std::map<std::string, std::string>& SegmentInfo::GetDescriptions() const { return descriptions; }
Locator SegmentInfo::GetLocator() const { return locatorGuard.Get(); }
uint64_t SegmentInfo::GetDocCount() const { return docCount; }
int64_t SegmentInfo::GetTimestamp() const { return timestamp; }
uint32_t SegmentInfo::GetMaxTTL() const { return maxTTL; }
uint32_t SegmentInfo::GetShardCount() const { return shardCount; }
uint32_t SegmentInfo::GetShardId() const { return shardId; }
schemaid_t SegmentInfo::GetSchemaId() const { return schemaId; }
bool SegmentInfo::IsMergedSegment() const { return mergedSegment; }

void SegmentInfo::SetTimestamp(int64_t timestamp_) { timestamp = timestamp_; }
void SegmentInfo::SetShardId(uint32_t shardId_) { shardId = shardId_; }
void SegmentInfo::SetShardCount(uint32_t shardCount_) { shardCount = shardCount_; }
void SegmentInfo::SetMergedSegment(bool isMergedSegment) { mergedSegment = isMergedSegment; }
void SegmentInfo::SetLocator(const Locator& locator_) { locatorGuard.Set(locator_); }
std::pair<Status, SegmentStatistics> SegmentInfo::GetSegmentStatistics() const
{
    std::string statStr;
    if (!GetDescription(SegmentStatistics::JSON_KEY, statStr)) {
        return {Status::OK(), SegmentStatistics()};
    }
    return indexlib::file_system::JsonUtil::FromString<SegmentStatistics>(statStr).StatusWith();
}
void SegmentInfo::SetSegmentStatistics(const SegmentStatistics& segStats)
{
    AddDescription(SegmentStatistics::JSON_KEY, autil::legacy::ToJsonString(segStats));
}

} // namespace indexlibv2::framework
