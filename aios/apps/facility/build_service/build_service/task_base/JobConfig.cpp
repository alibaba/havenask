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
#include "build_service/task_base/JobConfig.h"

#include "autil/StringUtil.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/RangeUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, JobConfig);

JobConfig::JobConfig()
    : _generationId(INVALID_GENERATIONID)
    , _buildPartitionFrom(0)
    , _buildPartitionCount(0)
    , _amonitorPort(10086)
{
}

JobConfig::~JobConfig() {}

bool JobConfig::init(const KeyValueMap& kvMap)
{
    _clusterName = getValueFromKeyValueMap(kvMap, CLUSTER_NAMES);
    if (_clusterName.empty()) {
        string errorMsg = "Invalid input : cluster name is empty!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    initBuildRuleConfig(kvMap);

    _buildPartitionFrom = getValueWithDefault<int32_t>(kvMap, BUILD_PARTITION_FROM, 0);
    _buildPartitionCount =
        getValueWithDefault<int32_t>(kvMap, BUILD_PARTITION_COUNT, _config.partitionCount - _buildPartitionFrom);
    _generationId = getValueWithDefault<generationid_t>(kvMap, GENERATION_ID, INVALID_GENERATIONID);
    if (_generationId == INVALID_GENERATIONID) {
        string errorMsg = "failed to get generation id!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _buildMode = getValueFromKeyValueMap(kvMap, BUILD_MODE);
    if (_buildMode != BUILD_MODE_INCREMENTAL && _buildMode != BUILD_MODE_FULL) {
        string errorMsg = "unknown build mode [" + _buildMode + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (_config.partitionCount < _buildPartitionFrom + _buildPartitionCount) {
        stringstream ss;
        ss << "Invalid buildPartitionFrom and buildPartitionCount,"
           << "buildPartitionFrom[" << _buildPartitionFrom << "]+buildPartitionCount[" << _buildPartitionCount
           << "] should not greater than partitionCount[" << _config.partitionCount << "]!";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _amonitorPort = getValueWithDefault<uint16_t>(kvMap, AMON_PORT, _amonitorPort);

    return true;
}

void JobConfig::initBuildRuleConfig(const KeyValueMap& kvMap)
{
    _config.partitionCount = getValueWithDefault<uint32_t>(kvMap, PARTITION_COUNT, 1);
    _config.buildParallelNum = getValueWithDefault<uint32_t>(kvMap, BUILD_PARALLEL_NUM, 1);
    _config.mergeParallelNum = getValueWithDefault<uint32_t>(kvMap, MERGE_PARALLEL_NUM, 1);
    _config.mapReduceRatio = getValueWithDefault<uint32_t>(kvMap, MAP_REDUCE_RATIO, 1);
    _config.partitionSplitNum = getValueWithDefault<uint32_t>(kvMap, PART_SPLIT_NUM, 1);
    _config.needPartition = getValueWithDefault<bool>(kvMap, NEED_PARTITION);
}

Range JobConfig::calculateBuildMapRange(uint32_t instanceId) const
{
    vector<Range> buildParts = splitBuildParts();
    if (!_config.needPartition) {
        uint32_t globalInstanceId = instanceId + _buildPartitionFrom * _config.buildParallelNum;
        assert(globalInstanceId < buildParts.size());
        return buildParts[globalInstanceId];
    }
    uint32_t partId = instanceId / _config.mapReduceRatio;
    uint32_t idxInPart = instanceId % _config.mapReduceRatio;
    assert(partId < buildParts.size());
    vector<Range> rangeVec =
        RangeUtil::splitRange(buildParts[partId].from(), buildParts[partId].to(), _config.mapReduceRatio);
    return rangeVec[idxInPart];
}

Range JobConfig::calculateBuildReduceRange(uint32_t instanceId) const
{
    assert(_config.needPartition);
    vector<Range> buildParts = splitBuildParts();
    uint32_t globalInstanceId = instanceId + _buildPartitionFrom * _config.buildParallelNum;
    assert(globalInstanceId < buildParts.size());
    return buildParts[globalInstanceId];
}

Range JobConfig::calculateMergeMapRange(uint32_t instanceId) const
{
    vector<Range> parts = splitBuildParts();
    uint32_t originBuildParallelNum = _config.buildParallelNum / _config.partitionSplitNum;
    uint32_t first = (instanceId + _buildPartitionFrom * _config.partitionSplitNum) * originBuildParallelNum;
    uint32_t last = first + originBuildParallelNum - 1;
    assert(parts.size() > first);
    assert(parts.size() > last);
    Range range;
    range.set_from(parts[first].from());
    range.set_to(parts[last].to());
    return range;
}

Range JobConfig::calculateMergeReduceRange(uint32_t instanceId) const
{
    return calculateMergeMapRange(instanceId / _config.mergeParallelNum);
}

vector<int32_t> JobConfig::calcMapToReduceTable() const
{
    return calcMapToReduceTable(_buildPartitionFrom, _buildPartitionCount, _config.partitionCount,
                                _config.buildParallelNum);
}

vector<Range> JobConfig::getAllNeedMergePart(const Range& mergeRange) const
{
    vector<Range> buildParts = splitBuildParts();
    vector<Range> mergeParts;
    for (size_t i = 0; i < buildParts.size(); ++i) {
        if (buildParts[i].from() < mergeRange.from()) {
            continue;
        }
        if (buildParts[i].from() > mergeRange.to()) {
            break;
        }
        mergeParts.push_back(buildParts[i]);
    }
    return mergeParts;
}

vector<Range> JobConfig::splitBuildParts() const
{
    vector<Range> rangeVec = splitFinalParts();
    if (_config.buildParallelNum == 1) {
        return rangeVec;
    }
    vector<Range> parallelRangeVec;
    for (size_t i = 0; i < rangeVec.size(); ++i) {
        vector<Range> onePart = RangeUtil::splitRange(rangeVec[i].from(), rangeVec[i].to(), _config.buildParallelNum);
        parallelRangeVec.insert(parallelRangeVec.end(), onePart.begin(), onePart.end());
    }
    return parallelRangeVec;
}

vector<Range> JobConfig::splitFinalParts() const
{
    return RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, _config.partitionCount);
}

vector<int32_t> JobConfig::calcMapToReduceTable(int32_t buildPartFrom, int32_t buildPartCount, int32_t partitionCount,
                                                int32_t buildParallelNum)
{
    assert(buildPartFrom + buildPartCount <= partitionCount);
    vector<Range> partRangeVec = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, partitionCount);
    partRangeVec.assign(partRangeVec.begin() + buildPartFrom, partRangeVec.begin() + buildPartFrom + buildPartCount);
    vector<Range> reducePartRangeVec;
    for (size_t i = 0; i < partRangeVec.size(); ++i) {
        vector<Range> parallelPartRanges =
            RangeUtil::splitRange(partRangeVec[i].from(), partRangeVec[i].to(), buildParallelNum);
        reducePartRangeVec.insert(reducePartRangeVec.end(), parallelPartRanges.begin(), parallelPartRanges.end());
    }
    vector<int32_t> table(RANGE_MAX + 1, -1);
    for (size_t reduceId = 0; reduceId < reducePartRangeVec.size(); ++reduceId) {
        for (uint32_t hashId = reducePartRangeVec[reduceId].from(); hashId <= reducePartRangeVec[reduceId].to();
             ++hashId) {
            table[hashId] = reduceId;
        }
    }
    return table;
}

template <typename T>
T JobConfig::getValueWithDefault(const KeyValueMap& kvMap, const string& keyName, const T& defaultValue)
{
    string valueStr = getValueFromKeyValueMap(kvMap, keyName);
    if (valueStr.empty()) {
        return defaultValue;
    }
    T value;
    if (!StringUtil::fromString(valueStr.c_str(), value)) {
        string errorMsg = "Invalid " + keyName + "[" + valueStr + "], must be integer.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return defaultValue;
    }
    return value;
}

}} // namespace build_service::task_base
