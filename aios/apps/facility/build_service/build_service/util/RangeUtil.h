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
#include <utility>
#include <vector>

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"

namespace swift { namespace network {
class SwiftAdminAdapter;
}} // namespace swift::network

namespace build_service { namespace util {

class RangeUtil
{
private:
    RangeUtil();
    ~RangeUtil();
    RangeUtil(const RangeUtil&);
    RangeUtil& operator=(const RangeUtil&);

public:
    static std::vector<proto::Range> splitRange(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount);
    static std::vector<proto::Range> splitRange(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount,
                                                uint32_t parallelNum);

    static bool getParallelInfoByRange(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount,
                                       uint32_t parallelNum, const proto::Range& range, uint32_t& partitionIdx,
                                       uint32_t& parallelIdx);

    static proto::Range getRangeInfo(uint32_t rangeFrom, uint32_t rangeTo, uint16_t partitionCount,
                                     uint32_t parallelNum, uint32_t partitionIdx, uint32_t parallelIdx);

    static uint16_t getRangeId(uint32_t id, uint32_t count);

    static bool getPartitionCount(swift::network::SwiftAdminAdapter* swiftAdapter, const std::string& topicName,
                                  int64_t* partitionCount);

    static std::vector<proto::Range> splitRangeBySwiftPartition(uint32_t rangeFrom, uint32_t rangeTo,
                                                                uint32_t totalSwiftPartitionCount,
                                                                uint32_t parallelNum);

    static bool getPartitionRange(const std::string& indexRoot,
                                  std::vector<std::pair<std::pair<uint32_t, uint32_t>, std::string>>& rangePath);

private:
    static proto::Range getRangeBySwiftPartitionId(uint32_t minRange, uint32_t maxRange, uint32_t startSwiftPartitionId,
                                                   uint32_t endSwiftPartitionId, uint32_t swiftPartitionRangeLength);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RangeUtil);

}} // namespace build_service::util
