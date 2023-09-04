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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

struct ProcessorBasicInfo {
    ProcessorBasicInfo() = default;
    ProcessorBasicInfo(int64_t src, int64_t offset, uint32_t partitionCount, uint32_t parallelNum)
        : src(src)
        , offset(offset)
        , partitionCount(partitionCount)
        , parallelNum(parallelNum)
    {
    }
    bool checkSourceAndPartition(ProcessorBasicInfo latestInfo)
    {
        return src != latestInfo.src || partitionCount != latestInfo.partitionCount ||
               parallelNum != latestInfo.parallelNum;
    }
    int64_t src = -1;
    int64_t offset = -1;
    uint32_t partitionCount = 0;
    uint32_t parallelNum = 0;
};

struct ProcessorTargetInfo {
    ProcessorTargetInfo() = default;
    ProcessorTargetInfo(int64_t offset, const std::string& userData) : offset(offset), userData(userData) {}
    int64_t offset = -1;
    std::string userData;
};

class ProcessorTargetInfos : public autil::legacy::Jsonizable
{
public:
    ProcessorTargetInfos();
    ~ProcessorTargetInfos();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    ProcessorTargetInfo get(size_t processorIdx) const;
    void update(const ProcessorBasicInfo& basicInfo, const std::vector<ProcessorTargetInfo>& targetInfo);

private:
    std::pair<int32_t, std::string> serilize() const;
    void deserilize(const std::string& str, int32_t serilizeMode);

public:
    static const int32_t INVALID_MODE;

private:
    ProcessorBasicInfo _processorBasicInfo;
    std::vector<ProcessorTargetInfo> _processorTargetInfos;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorTargetInfos);

}} // namespace build_service::admin
