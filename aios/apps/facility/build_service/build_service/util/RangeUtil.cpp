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
#include "build_service/util/RangeUtil.h"

#include <math.h>
#include <string>

#include "autil/KsHashFunction.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "swift/common/RangeUtil.h"
#include "swift/network/SwiftAdminAdapter.h"

using namespace std;

namespace build_service { namespace util {

BS_LOG_SETUP(util, RangeUtil);

std::vector<proto::Range> RangeUtil::splitRange(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount)
{
    assert(rangeTo <= 65535);

    std::vector<proto::Range> ranges;
    if (partitionCount == 0 || rangeFrom > rangeTo) {
        return ranges;
    }

    uint32_t rangeCount = rangeTo - rangeFrom + 1;

    uint32_t c = rangeCount / partitionCount;
    uint32_t m = rangeCount % partitionCount;

    uint32_t from = rangeFrom;
    for (uint32_t i = 0; i < partitionCount && from <= rangeTo; ++i) {
        uint32_t to = from + c + (i >= m ? 0 : 1) - 1;
        proto::Range range;
        range.set_from(from);
        range.set_to(to);
        ranges.push_back(range);
        from = to + 1;
    }

    return ranges;
}

std::vector<proto::Range> RangeUtil::splitRange(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount,
                                                uint32_t parallelNum)
{
    std::vector<proto::Range> ranges = splitRange(rangeFrom, rangeTo, partitionCount);
    if (ranges.empty() || parallelNum == 1) {
        return ranges;
    }

    std::vector<proto::Range> ret;
    for (size_t i = 0; i < ranges.size(); i++) {
        std::vector<proto::Range> subRanges = splitRange(ranges[i].from(), ranges[i].to(), parallelNum);
        ret.insert(ret.end(), subRanges.begin(), subRanges.end());
    }
    return ret;
}

static bool RangeLessComp(const proto::Range& lft, const proto::Range& rht)
{
    return lft.from() < rht.from() && lft.to() < rht.to();
}

proto::Range RangeUtil::getRangeInfo(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount,
                                     uint32_t parallelNum, uint32_t partitionIdx, uint32_t parallelIdx)
{
    std::vector<proto::Range> ranges = splitRange(rangeFrom, rangeTo, partitionCount);
    proto::Range partRange = ranges[partitionIdx];
    std::vector<proto::Range> instanceRanges = splitRange(partRange.from(), partRange.to(), parallelNum);
    return instanceRanges[parallelIdx];
}

bool RangeUtil::getParallelInfoByRange(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount,
                                       uint32_t parallelNum, const proto::Range& range, uint32_t& partitionIdx,
                                       uint32_t& parallelIdx)
{
    if (range.from() >= range.to()) {
        return false;
    }

    std::vector<proto::Range> ranges = splitRange(rangeFrom, rangeTo, partitionCount);
    for (uint32_t i = 0; i < ranges.size(); i++) {
        const proto::Range& partRange = ranges[i];
        if (range.from() < partRange.from() || range.to() > partRange.to()) {
            continue;
        }

        std::vector<proto::Range> subRanges = splitRange(partRange.from(), partRange.to(), parallelNum);
        std::vector<proto::Range>::iterator iter =
            lower_bound(subRanges.begin(), subRanges.end(), range, RangeLessComp);
        if (iter != subRanges.end() && iter->from() == range.from() && iter->to() == range.to()) {
            partitionIdx = i;
            parallelIdx = distance(subRanges.begin(), iter);
            return true;
        }
    }
    return false;
}

uint16_t RangeUtil::getRangeId(uint32_t id, uint32_t count)
{
    uint32_t fullRange = 65536;
    uint32_t hashid = autil::KsHashFunction::getHashId(id, count, fullRange);
    uint16_t rangeId = hashid % 65536;
    return rangeId;
}

bool RangeUtil::getPartitionCount(swift::network::SwiftAdminAdapter* swiftAdapter, const std::string& topicName,
                                  int64_t* partitionCount)
{
    uint32_t swiftPartitionCount = 0;
    swift::protocol::ErrorCode ec = swiftAdapter->getPartitionCount(topicName, swiftPartitionCount);
    if (ec == swift::protocol::ERROR_NONE) {
        *partitionCount = swiftPartitionCount;
        return true;
    }

    BS_LOG(ERROR, "getPartitionCount for topic [%s] fail, error [%s]", topicName.c_str(),
           swift::protocol::ErrorCode_Name(ec).c_str());
    return false;
}

proto::Range RangeUtil::getRangeBySwiftPartitionId(uint32_t minRange, uint32_t maxRange, uint32_t startSwiftPartitionId,
                                                   uint32_t endSwiftPartitionId, uint32_t swiftPartitionRangeLength)
{
    proto::Range range;
    uint32_t rangeFrom = swiftPartitionRangeLength * startSwiftPartitionId;
    if (rangeFrom < minRange) {
        rangeFrom = minRange;
    }
    uint32_t rangeTo = swiftPartitionRangeLength * (endSwiftPartitionId + 1) - 1;
    if (rangeTo > maxRange) {
        rangeTo = maxRange;
    }

    range.set_from(rangeFrom);
    range.set_to(rangeTo);
    return range;
}

// Swift doesn't support reading swift partition in parallel efficiently, thus we only assign at most one reader to each
// swift partition.
// Assumption here is RANGE_MIN = 0, RANGE_MAX = 65535.
std::vector<proto::Range> RangeUtil::splitRangeBySwiftPartition(uint32_t rangeFrom, uint32_t rangeTo,
                                                                uint32_t totalSwiftPartitionCount, uint32_t parallelNum)
{
    std::vector<proto::Range> ranges;
    if (totalSwiftPartitionCount == 0 or (RANGE_MAX + 1) % totalSwiftPartitionCount != 0) {
        BS_LOG(ERROR, "Invalid swift partition count %u", totalSwiftPartitionCount);
        return ranges;
    }
    if (rangeFrom > rangeTo or rangeTo > RANGE_MAX) {
        BS_LOG(ERROR, "Invalid range [%u, %u]", rangeFrom, rangeTo);
        return ranges;
    }

    auto swiftRangeUtil = std::make_unique<swift::common::RangeUtil>(totalSwiftPartitionCount);
    std::vector<uint32_t> actualSwiftPartitionIds = swiftRangeUtil->getPartitionIds(rangeFrom, rangeTo);
    size_t swiftPartitionCount = actualSwiftPartitionIds.size();
    const uint32_t swiftPartitionRangeLength = (RANGE_MAX + 1) / totalSwiftPartitionCount;
    if (parallelNum > swiftPartitionCount) {
        BS_LOG(WARN, "parallelNum [%u] is greater than swift partition count [%lu], will set parallel number to [%lu]",
               parallelNum, swiftPartitionCount, swiftPartitionCount);
        for (int i = 0; i < swiftPartitionCount; ++i) {
            ranges.push_back(getRangeBySwiftPartitionId(rangeFrom, rangeTo,
                                                        /*startSwiftPartitionId=*/actualSwiftPartitionIds[i],
                                                        /*endSwiftPartitionId=*/actualSwiftPartitionIds[i],
                                                        swiftPartitionRangeLength));
        }
        return ranges;
    }
    // unsigned integer division will round down towards 0.
    uint32_t swiftPartitionsPerParallel = swiftPartitionCount / parallelNum;
    uint32_t swiftPartitionsPerParallelRemainder = swiftPartitionCount % parallelNum;
    uint32_t startSwiftPartitionId = actualSwiftPartitionIds[0];
    uint32_t endSwiftPartitionId = 0;
    for (int i = 0; i < parallelNum; ++i) {
        endSwiftPartitionId =
            startSwiftPartitionId + swiftPartitionsPerParallel - 1 + (i < swiftPartitionsPerParallelRemainder ? 1 : 0);
        ranges.push_back(getRangeBySwiftPartitionId(rangeFrom, rangeTo, startSwiftPartitionId, endSwiftPartitionId,
                                                    swiftPartitionRangeLength));
        startSwiftPartitionId = endSwiftPartitionId + 1;
    }
    return ranges;
}

bool RangeUtil::getPartitionRange(const string& indexRoot, vector<pair<pair<uint32_t, uint32_t>, string>>& rangesPath)
{
    vector<string> generationFileList;
    rangesPath.clear();
    if (fslib::util::FileUtil::listDir(indexRoot, generationFileList)) {
        for (const string& path : generationFileList) {
            pair<uint32_t, uint32_t> range;
            if (IndexPathConstructor::parsePartitionRange(path, range.first, range.second)) {
                rangesPath.push_back(make_pair(range, fslib::util::FileUtil::joinFilePath(indexRoot, path)));
            }
        }
    }
    sort(rangesPath.begin(), rangesPath.end());
    if (rangesPath.empty()) {
        BS_LOG(ERROR, "ranges is empty");
        return false;
    }
    if (rangesPath[0].first.first != RANGE_MIN || rangesPath[rangesPath.size() - 1].first.second != RANGE_MAX) {
        BS_LOG(ERROR, "illegal range [%u, %u]", rangesPath[0].first.first,
               rangesPath[rangesPath.size() - 1].first.second);
        return false;
    }
    for (size_t i = 1; i < rangesPath.size(); ++i) {
        if (rangesPath[i].first.first != rangesPath[i - 1].first.second + 1) {
            BS_LOG(ERROR, "broken range [%u] [%u]", rangesPath[i - 1].first.second, rangesPath[i].first.first);
            return false;
        }
    }
    return true;
}

}} // namespace build_service::util
