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
#include "indexlib/framework/LevelInfo.h"

#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/util/ShardUtil.h"

using namespace std;
using namespace indexlib::util;

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, LevelInfo);

LevelMeta::LevelMeta() : levelIdx(0), cursor(0), topology(topo_sequence) {}

LevelMeta::~LevelMeta() {}

bool LevelMeta::RemoveSegment(segmentid_t segmentId)
{
    for (auto iter = segments.begin(); iter != segments.end(); ++iter) {
        if (*iter == segmentId) {
            switch (topology) {
            case topo_sequence:
            case topo_hash_range:
                segments.erase(iter);
                break;
            case topo_hash_mod:
                *iter = INVALID_SEGMENTID;
                break;
            default:
                assert(false);
            }
            return true;
        }
    }
    return false;
}

void LevelMeta::Clear()
{
    switch (topology) {
    case topo_sequence:
    case topo_hash_range:
        segments.clear();
        break;
    case topo_hash_mod: {
        vector<segmentid_t> emptySegments(segments.size(), INVALID_SEGMENTID);
        segments.swap(emptySegments);
        break;
    }
    case topo_default:
        assert(false);
    }
}

void LevelMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("level_idx", levelIdx);
    json.Jsonize("cursor", cursor);
    json.Jsonize("segments", segments);

    if (json.GetMode() == TO_JSON) {
        string topoStr = TopologyToStr(topology);
        json.Jsonize("topology", topoStr);
    } else {
        string topoStr;
        json.Jsonize("topology", topoStr);
        topology = StrToTopology(topoStr);
    }
}

bool LevelMeta::operator==(const LevelMeta& other) const
{
    return (levelIdx == other.levelIdx) && (topology == other.topology) && (segments == other.segments);
}

LevelInfo::LevelInfo() {}

LevelInfo::~LevelInfo() {}

void LevelInfo::Jsonize(JsonWrapper& json) { json.Jsonize("level_metas", levelMetas); }

void LevelInfo::Init(LevelTopology topo, uint32_t levelNum, size_t shardCount)
{
    levelMetas.clear();

    AddLevel(topo_sequence);
    for (uint32_t i = 1; i < levelNum; ++i) {
        AddLevel(topo);
        switch (topo) {
        case topo_hash_mod:
            levelMetas[i].segments.resize(shardCount, INVALID_SEGMENTID);
            break;
        case topo_sequence:
            break;
        case topo_hash_range:
        case topo_default:
            assert(false);
            break;
        }
    }
}

void LevelInfo::AddLevel(LevelTopology levelTopo)
{
    LevelMeta levelMeta;
    levelMeta.levelIdx = levelMetas.size();
    levelMeta.topology = levelTopo;
    levelMetas.push_back(levelMeta);
}

void LevelInfo::AddSegment(segmentid_t segmentId)
{
    levelMetas[0].segments.push_back(segmentId); // New segments push to Level 0, for build
}

Status LevelInfo::Import(const std::vector<std::shared_ptr<LevelInfo>>& otherLevelInfos,
                         const std::set<segmentid_t>& newSegmentsHint)
{
    if (otherLevelInfos.empty()) {
        AUTIL_LOG(ERROR, "has no other level info import failed!");
        return Status::InvalidArgs();
    }
    std::set<segmentid_t> segmentIds;
    for (const auto& levelMeta : levelMetas) {
        for (segmentid_t segmentId : levelMeta.segments) {
            segmentIds.insert(segmentId);
        }
    }
    std::vector<LevelMeta> baseMetas = levelMetas;
    for (const auto& otherLevelInfo : otherLevelInfos) {
        // check other level info
        if (GetTopology() != otherLevelInfo->GetTopology()) {
            AUTIL_LOG(ERROR, "Base levelInfo topology [%s] != other levelInfo topology [%s]. Import failed!",
                      LevelMeta::TopologyToStr(GetTopology()).c_str(),
                      LevelMeta::TopologyToStr(otherLevelInfo->GetTopology()).c_str());
            return Status::InvalidArgs();
        }
        if (GetLevelCount() < otherLevelInfo->GetLevelCount()) {
            AUTIL_LOG(ERROR, "Base levelInfo levelCount [%lu] < other levelInfo levelCount [%lu]. Import failed!",
                      GetLevelCount(), otherLevelInfo->GetLevelCount());
            return Status::InvalidArgs();
        }
        if (GetShardCount() != otherLevelInfo->GetShardCount()) {
            AUTIL_LOG(ERROR, "Base levelInfo shardCount [%lu] != other levelInfo shardCount [%lu]. Import failed!",
                      GetShardCount(), otherLevelInfo->GetShardCount());
            return Status::InvalidArgs();
        }
        // import level
        for (size_t i = 0; i < otherLevelInfo->GetLevelCount(); i++) {
            auto& baseMeta = baseMetas[i];
            const auto& newMeta = otherLevelInfo->levelMetas[i];
            if (baseMeta.topology != newMeta.topology) {
                AUTIL_LOG(ERROR, "base level topology[%s] and new level topology[%s] don't match",
                          LevelMeta::TopologyToStr(baseMeta.topology).c_str(),
                          LevelMeta::TopologyToStr(newMeta.topology).c_str());
                return Status::InvalidArgs();
            }
            if (baseMeta.topology == topo_sequence) {
                auto status = ImportTopoSequenceLevel(newSegmentsHint, newMeta, baseMeta, segmentIds);
                if (!status.IsOK()) {
                    return status;
                }
            } else if (baseMeta.topology == topo_hash_mod) {
                auto status = ImportTopoHashLevel(newSegmentsHint, newMeta, baseMeta, segmentIds);
                if (!status.IsOK()) {
                    return status;
                }
            } else {
                AUTIL_LOG(ERROR, "topology [%s] import not support",
                          LevelMeta::TopologyToStr(baseMeta.topology).c_str());
                assert(false);
                return Status::Unimplement();
            }
        }
    }
    levelMetas.swap(baseMetas);
    return Status::OK();
}

Status LevelInfo::ImportTopoSequenceLevel(const std::set<segmentid_t>& newSegmentsHint, const LevelMeta& newMeta,
                                          LevelMeta& baseMeta, std::set<segmentid_t>& segmentIds)
{
    for (size_t j = 0; j < newMeta.segments.size(); j++) {
        segmentid_t segmentId = newMeta.segments[j];
        if (segmentId == INVALID_SEGMENTID) {
            continue;
        }
        if (newSegmentsHint.count(segmentId) == 0) {
            // not in new segments hint, pass
            continue;
        }
        if (segmentIds.count(segmentId) == 0) {
            baseMeta.segments.push_back(segmentId);
            segmentIds.insert(segmentId);
        }
    }
    return Status::OK();
}

Status LevelInfo::ImportTopoHashLevel(const std::set<segmentid_t>& newSegmentsHint, const LevelMeta& newMeta,
                                      LevelMeta& baseMeta, std::set<segmentid_t>& segmentIds)
{
    for (size_t j = 0; j < newMeta.segments.size(); j++) {
        if (baseMeta.segments.size() != newMeta.segments.size()) {
            AUTIL_LOG(ERROR,
                      "topo_hash_mod base level segments count [%lu]"
                      " not equal to new level segments count [%lu]",
                      baseMeta.segments.size(), newMeta.segments.size());
            return Status::InvalidArgs();
        }
        segmentid_t segmentId = newMeta.segments[j];
        if (segmentId == INVALID_SEGMENTID) {
            continue;
        }
        if (newSegmentsHint.count(segmentId) == 0) {
            // not in new segments hint, pass
            continue;
        }
        if (segmentIds.count(segmentId) == 0) {
            if (baseMeta.segments[j] == INVALID_SEGMENTID) {
                baseMeta.segments[j] = segmentId;
                segmentIds.insert(segmentId);
            } else {
                AUTIL_LOG(ERROR,
                          "segmentId[%d] add failed, position [%lu]"
                          " has already been occupied by segment[%d]",
                          segmentId, j, baseMeta.segments[j]);
                return Status::InvalidArgs();
            }
        } else {
            if (baseMeta.segments[j] != segmentId) {
                AUTIL_LOG(ERROR, "segmentId[%d] already exist in other position", segmentId);
                return Status::InvalidArgs();
            }
        }
    }
    return Status::OK();
}

void LevelInfo::RemoveSegment(segmentid_t segmentId)
{
    for (auto& levelMeta : levelMetas) {
        if (levelMeta.RemoveSegment(segmentId)) {
            return;
        }
    }
}

bool LevelInfo::FindPosition(segmentid_t segmentId, uint32_t& levelIdx, uint32_t& inLevelIdx) const
{
    for (size_t i = 0; i < levelMetas.size(); ++i) {
        const vector<segmentid_t>& segments = levelMetas[i].segments;
        for (size_t j = 0; j < segments.size(); ++j) {
            if (segments[j] == segmentId) {
                levelIdx = i;
                inLevelIdx = j;
                return true;
            }
        }
    }
    return false;
}

void LevelInfo::Clear()
{
    for (auto& levelMeta : levelMetas) {
        levelMeta.Clear();
    }
}

LevelTopology LevelInfo::GetTopology() const
{
    return levelMetas.empty() ? topo_sequence : levelMetas.crbegin()->topology;
}

size_t LevelInfo::GetShardCount() const
{
    size_t ret = 1;
    for (auto& levelMeta : levelMetas) {
        if (levelMeta.topology == topo_hash_mod) {
            ret = std::max(ret, levelMeta.segments.size());
        }
    }
    return ret;
}

vector<segmentid_t> LevelInfo::GetSegmentIds(size_t globalShardId) const
{
    vector<segmentid_t> ret;
    size_t globalShardCount = GetShardCount();
    for (auto& levelMeta : levelMetas) {
        if (levelMeta.topology == topo_sequence || levelMeta.topology == topo_hash_range) {
            ret.insert(ret.end(), levelMeta.segments.rbegin(), levelMeta.segments.rend());
        } else if (levelMeta.topology == topo_hash_mod) {
            size_t currentShardCount = levelMeta.segments.size();
            size_t currentShardId =
                indexlib::util::ShardUtil::TransformShardId(globalShardId, currentShardCount, globalShardCount);
            segmentid_t segmentId = levelMeta.segments[currentShardId];
            if (segmentId != INVALID_SEGMENTID) {
                ret.push_back(segmentId);
            }
        } else {
            assert(false);
        }
    }
    return ret;
}

std::string LevelMeta::TopologyToStr(LevelTopology topology)
{
    switch (topology) {
    case topo_sequence:
        return TOPOLOGY_SEQUENCE_STR;
    case topo_hash_mod:
        return TOPOLOGY_HASH_MODE_STR;
    case topo_hash_range:
        return TOPOLOGY_HASH_RANGE_STR;
    case topo_default:
        return TOPOLOGY_DEFAULT_STR;
    }
    assert(false);
    return "unknown";
}

LevelTopology LevelMeta::StrToTopology(const std::string& str)
{
    if (str == TOPOLOGY_SEQUENCE_STR) {
        return topo_sequence;
    } else if (str == TOPOLOGY_HASH_MODE_STR) {
        return topo_hash_mod;
    } else if (str == TOPOLOGY_HASH_RANGE_STR) {
        return topo_hash_range;
    } else if (str == TOPOLOGY_DEFAULT_STR) {
        return topo_default;
    }
    assert(false);
    return topo_default;
}

} // namespace indexlibv2::framework
