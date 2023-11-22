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

#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib {
namespace file_system {
class Directory;
}

namespace index_base {

struct SegmentInfo : public indexlibv2::framework::SegmentInfo {
public:
    SegmentInfo() = default;
    explicit SegmentInfo(const indexlibv2::framework::SegmentInfo& base) : indexlibv2::framework::SegmentInfo(base) {}
    SegmentInfo& operator=(const indexlibv2::framework::SegmentInfo& other);
    bool operator==(const SegmentInfo& other) const;
    bool operator!=(const SegmentInfo& other) const { return !(*this == other); }

    void Update(const document::DocumentPtr& doc);
    bool GetCompressFingerPrint(uint64_t& fingerPrint) const;
    bool GetOriginalTemperatureMeta(SegmentTemperatureMeta& meta) const;

    std::string ToString(bool compactFormat = false) const;
    void FromString(const std::string& str);

    bool Load(const std::shared_ptr<indexlib::file_system::Directory>& dir);
    void Store(const std::shared_ptr<indexlib::file_system::Directory>& directory, bool removeIfExist = false) const;

private:
    void TEST_Store(const std::string& path) const;
    void TEST_Load(std::string path);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentInfo);
typedef std::vector<SegmentInfo> SegmentInfos;

//////////////////////////////////////////////
inline bool SegmentInfo::operator==(const SegmentInfo& other) const
{
    if (this == &other) {
        return true;
    }
    return ((other.docCount == docCount) && (other.GetLocator() == locatorGuard.Get()) &&
            (other.timestamp == timestamp) && (other.mergedSegment == mergedSegment) && (other.maxTTL == maxTTL) &&
            (other.shardCount == shardCount) && (other.shardId == shardId) && (other.schemaId == schemaId) &&
            (other.descriptions == descriptions));
}

inline std::ostream& operator<<(std::ostream& os, const SegmentInfo& segInfo)
{
    os << segInfo.ToString();
    return os;
}
} // namespace index_base
} // namespace indexlib
