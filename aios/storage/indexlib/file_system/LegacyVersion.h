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

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib { namespace file_system {

// This class is used inside file system to parse Version(indexlib/index_base/index_meta/version.h)
// files for compatibility reasons. Only backward compatible code paths might trigger this class.
// This functionality this class provides is only a subset of the Version class as needed by file
// system, but this subset should behave the same as the Version class. Ideally this file should
// be removed after the index versions become the same.
class LegacyVersion : public autil::legacy::Jsonizable
{
private:
    using segmentid_t = int32_t;
    class LevelMeta : public autil::legacy::Jsonizable
    {
    public:
        LevelMeta() {}
        ~LevelMeta() {}

    public:
        void Jsonize(JsonWrapper& json) override;

    public:
        const std::vector<segmentid_t>& GetSegments() const;

    private:
        std::vector<segmentid_t> _segments;
    };
    class LevelInfo : public autil::legacy::Jsonizable
    {
    public:
        LevelInfo() {}
        ~LevelInfo() {}

    public:
        void Jsonize(JsonWrapper& json) override;

    public:
        bool FindPosition(segmentid_t segmentId, uint32_t& levelIdx, uint32_t& inLevelIdx) const;

    private:
        std::vector<LevelMeta> levelMetas;
    };

public:
    LegacyVersion() {}
    ~LegacyVersion() {}

public:
    void Jsonize(JsonWrapper& json) override;

public:
    std::vector<segmentid_t> GetSegmentVector();
    std::string GetSegmentDirName(segmentid_t segId) const;

private:
    static void Validate(const std::vector<segmentid_t>& segmentIds);

private:
    std::vector<segmentid_t> _segmentIds;
    LevelInfo _levelInfo;
    bool _hasLevelInfo = false;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::file_system
