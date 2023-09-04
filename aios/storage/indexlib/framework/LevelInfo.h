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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {

enum LevelTopology {
    topo_default = 0x00,
    topo_sequence = 0x01,
    topo_hash_mod = 0x02,
    topo_key_range = 0x04,
};

class LevelMeta : public autil::legacy::Jsonizable
{
public:
    LevelMeta();
    ~LevelMeta();

    void Jsonize(JsonWrapper& json) override;

    static std::string TopologyToStr(LevelTopology topology);
    static LevelTopology StrToTopology(const std::string& str);
    bool RemoveSegment(segmentid_t segmentId);
    void Clear();

    bool operator==(const LevelMeta& other) const;

public:
    uint32_t levelIdx;
    uint32_t cursor;
    LevelTopology topology;
    std::vector<segmentid_t> segments;

public:
    inline static const std::string TOPOLOGY_DEFAULT_STR = "default";

private:
    inline static const std::string TOPOLOGY_SEQUENCE_STR = "sequence";
    inline static const std::string TOPOLOGY_HASH_MODE_STR = "hash_mod";
    inline static const std::string TOPOLOGY_KEY_RANGE_STR = "key_range";
};

class LevelInfo : public autil::legacy::Jsonizable
{
public:
    LevelInfo();
    ~LevelInfo();

    void Jsonize(JsonWrapper& json) override;
    void Init(LevelTopology topo, uint32_t levelNum, size_t shardCount = 1);

    void AddSegment(segmentid_t segmentId);
    bool AddSegment(segmentid_t segmentId, uint32_t levelIdx);
    void AddLevel(LevelTopology levelTopo);
    Status Import(const std::vector<std::shared_ptr<LevelInfo>>& otherLevelInfos,
                  const std::set<segmentid_t>& newSegmentHint);

    void RemoveSegment(segmentid_t segmentId);
    bool FindPosition(segmentid_t segmentId, uint32_t& levelIdx, uint32_t& inLevelIdx) const;
    void Clear();

    size_t GetLevelCount() const { return levelMetas.size(); }
    LevelTopology GetTopology() const;
    size_t GetShardCount() const;

    bool operator==(const LevelInfo& other) const { return levelMetas == other.levelMetas; }

    // return segmentids ordered from new to old
    std::vector<segmentid_t> GetSegmentIds(size_t shardId) const;

public:
    std::vector<LevelMeta> levelMetas;

private:
    Status ImportTopoSequenceLevel(const std::set<segmentid_t>& newSegmentsHint, const LevelMeta& newMeta,
                                   LevelMeta& baseMeta, std::set<segmentid_t>& segmentIds);
    Status ImportTopoHashLevel(const std::set<segmentid_t>& newSegmentsHint, const LevelMeta& newMeta,
                               LevelMeta& baseMeta, std::set<segmentid_t>& segmentIds);

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::framework
