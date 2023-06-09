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
#include "indexlib/table/normal_table/index_task/merger/NormalTabletMergeStrategyDefine.h"

#include "autil/StringUtil.h"

namespace indexlibv2 { namespace table {

AUTIL_LOG_SETUP(indexlib.table, InputLimits);
AUTIL_LOG_SETUP(indexlib.table, OutputLimits);

bool NormalTableMergeParam::ParseUint32Param(const std::string& kvPair, std::string& keyName, uint32_t& value)
{
    std::vector<std::string> strs;
    autil::StringUtil::fromString(kvPair, strs, "=");
    if (strs.size() != 2) {
        return false;
    }
    keyName = strs[0];
    return autil::StringUtil::fromString(strs[1], value);
}

//"input_limits" : "max-doc-count=100000000;max-valid-doc-count=80000000;max-total-merge-size=1024;"
//                 "max-segment-size=1024;max-valid-segment-size=1024"
bool InputLimits::FromString(const std::string& inputStr)
{
    std::vector<std::string> kvPairs;
    autil::StringUtil::fromString(inputStr, kvPairs, ";");
    for (size_t i = 0; i < kvPairs.size(); i++) {
        std::string key;
        uint32_t value;
        if (!ParseUint32Param(kvPairs[i], key, value)) {
            return false;
        }

        if (key == "max-doc-count") {
            maxDocCount = value;
            continue;
        }

        if (key == "max-valid-doc-count") {
            maxValidDocCount = value;
            continue;
        }

        if (key == "max-segment-size") {
            maxSegmentSize = (uint64_t)value * 1024 * 1024;
            continue;
        }

        if (key == "max-valid-segment-size") {
            maxValidSegmentSize = (uint64_t)value * 1024 * 1024;
            continue;
        }

        if (key == "max-total-merge-size") {
            maxTotalMergeSize = (uint64_t)value * 1024 * 1024;
            continue;
        }
        AUTIL_LOG(INFO, "Skipping parameter not intended for for priority queue merge strategy: [%s]:[%u]", key.c_str(),
                  value);
    }
    return true;
}

std::string InputLimits::ToString() const
{
    std::stringstream ss;
    ss << "max-doc-count=" << maxDocCount << ";"
       << "max-valid-doc-count=" << maxValidDocCount << ";"
       << "max-segment-size=" << maxSegmentSize / 1024 / 1024 << ";"
       << "max-valid-segment-size=" << maxValidSegmentSize / 1024 / 1024 << ";"
       << "max-total-merge-size=" << maxTotalMergeSize / 1024 / 1024;
    return ss.str();
}

bool PriorityDescription::DecodeIsAsc(const std::string& str)
{
    if (str == "asc") {
        isAsc = true;
        return true;
    }

    if (str == "desc") {
        isAsc = false;
        return true;
    }

    return false;
}

// priority-feature=doc_count#asc
void PriorityDescription::FromString(const std::string& str, bool* isPriorityDescription, bool* isValid)
{
    if (str.empty()) {
        *isPriorityDescription = true;
        *isValid = true;
        return;
    }

    std::vector<std::vector<std::string>> strs;
    autil::StringUtil::fromString(str, strs, "#", "=");

    if (strs.size() != 2 || strs[0].size() != 1 || strs[0][0] != "priority-feature") {
        *isPriorityDescription = false;
        *isValid = true;
        return;
    }

    *isPriorityDescription = true;
    if (strs[1][0] == "doc-count") {
        type = FE_DOC_COUNT;
        *isValid = strs[1].size() == 2 ? DecodeIsAsc(strs[1][1]) : true;
        return;
    }

    if (strs[1][0] == "delete-doc-count") {
        type = FE_DELETE_DOC_COUNT;
        *isValid = strs[1].size() == 2 ? DecodeIsAsc(strs[1][1]) : true;
        return;
    }

    if (strs[1][0] == "valid-doc-count") {
        type = FE_VALID_DOC_COUNT;
        *isValid = strs[1].size() == 2 ? DecodeIsAsc(strs[1][1]) : true;
        return;
    }

    if (strs[1][0] == "timestamp") {
        type = FE_TIMESTAMP;
        *isValid = strs[1].size() == 2 ? DecodeIsAsc(strs[1][1]) : true;
        return;
    }

    if (strs[1][0] == "temperature_conflict") {
        type = FE_DOC_TEMPERTURE_DIFFERENCE;
        *isValid = strs[1].size() == 2 ? DecodeIsAsc(strs[1][1]) : true;
        return;
    }

    *isValid = false;
    return;
}

std::string PriorityDescription::ToString() const
{
    std::stringstream ss;
    ss << "priority-feature=";

    if (type == FE_DOC_COUNT) {
        ss << "doc-count";
    }

    if (type == FE_VALID_DOC_COUNT) {
        ss << "valid-doc-count";
    }

    if (type == FE_DELETE_DOC_COUNT) {
        ss << "delete-doc-count";
    }

    if (type == FE_TIMESTAMP) {
        ss << "timestamp";
    }

    if (isAsc) {
        ss << "#asc";
    } else {
        ss << "#desc";
    }
    return ss.str();
}

// conflict-segment-count=5;conflict-delete-percent=60
void MergeTriggerDescription::FromString(const std::string& str, bool* isMergeTriggerDescription, bool* isValid)
{
    std::vector<std::string> kvPairs;
    autil::StringUtil::fromString(str, kvPairs, ";");
    *isMergeTriggerDescription = false;
    *isValid = true;
    for (size_t i = 0; i < kvPairs.size(); i++) {
        std::string key;
        uint32_t value;
        bool parseValue = ParseUint32Param(kvPairs[i], key, value);
        if (!(*isMergeTriggerDescription) and (key == "conflict-segment-count" or key == "conflict-delete-percent")) {
            *isMergeTriggerDescription = true;
        }
        if (key == "conflict-segment-count") {
            if (!parseValue) {
                *isValid = false;
                return;
            }
            conflictSegmentCount = std::max(1u, value);
            continue;
        }
        if (key == "conflict-delete-percent") {
            if (!parseValue) {
                *isValid = false;
                return;
            }
            if (value <= 100) {
                conflictDeletePercent = value;
                continue;
            } else {
                *isValid = false;
            }
        }
    }
    return;
}

std::string MergeTriggerDescription::ToString() const
{
    std::stringstream ss;
    ss << "conflict-segment-count=" << conflictSegmentCount << ";"
       << "conflict-delete-percent=" << conflictDeletePercent;
    return ss.str();
}

bool OutputLimits::FromString(const std::string& str)
{
    std::vector<std::string> kvPairs;
    autil::StringUtil::fromString(str, kvPairs, ";");
    for (size_t i = 0; i < kvPairs.size(); i++) {
        std::string key;
        uint32_t value;
        if (!ParseUint32Param(kvPairs[i], key, value)) {
            return false;
        }

        if (key == "max-merged-segment-doc-count") {
            maxMergedSegmentDocCount = value;
            continue;
        }

        if (key == "max-total-merge-doc-count") {
            maxTotalMergedDocCount = value;
            continue;
        }

        if (key == "max-merged-segment-size") {
            maxMergedSegmentSize = (uint64_t)value * 1024 * 1024;
            continue;
        }

        if (key == "max-total-merged-size") {
            maxTotalMergedSize = (uint64_t)value * 1024 * 1024;
            continue;
        }

        AUTIL_LOG(WARN, "Skipping parameter not intended for for priority queue merge strategy: [%s]:[%u]", key.c_str(),
                  value);
    }

    if (maxMergedSegmentDocCount > maxTotalMergedDocCount) {
        AUTIL_LOG(WARN,
                  "max-merged-segment-doc-count [%u] is bigger"
                  " than max-total-merge-doc-count[%u], "
                  "make max-merged-segment-doc-count equal"
                  " to max-total-merge-doc-count",
                  maxMergedSegmentDocCount, maxTotalMergedDocCount);
        maxMergedSegmentDocCount = maxTotalMergedDocCount;
    }

    if (maxMergedSegmentSize > maxTotalMergedSize) {
        AUTIL_LOG(WARN,
                  "max-merged-segment-size [%lu] is bigger"
                  " than max-total-merged-size [%lu], "
                  "make max-merged-segment-size equal"
                  " to max-total-merged-size",
                  maxMergedSegmentSize, maxTotalMergedSize);
        maxMergedSegmentSize = maxTotalMergedSize;
    }
    return true;
}

std::string OutputLimits::ToString() const
{
    std::stringstream ss;
    ss << "max-merged-segment-doc-count=" << maxMergedSegmentDocCount << ";"
       << "max-total-merge-doc-count=" << maxTotalMergedDocCount << ";"
       << "max-merged-segment-size=" << maxMergedSegmentSize << ";"
       << "max-total-merged-size=" << maxTotalMergedSize;
    return ss.str();
}
}} // namespace indexlibv2::table
