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
#include "build_service/admin/taskcontroller/ProcessorTargetInfos.h"

#include <assert.h>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <memory>

#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/jsonizable_exception.h"
#include "autil/legacy/legacy_jsonizable.h"

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProcessorTargetInfos);

ProcessorTargetInfos::ProcessorTargetInfos() {}

ProcessorTargetInfos::~ProcessorTargetInfos() {}

const int32_t ProcessorTargetInfos::INVALID_MODE = -1;

void ProcessorTargetInfos::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("src", _processorBasicInfo.src, _processorBasicInfo.src);
    json.Jsonize("offset", _processorBasicInfo.offset, _processorBasicInfo.offset);
    json.Jsonize("partitionCount", _processorBasicInfo.partitionCount, _processorBasicInfo.partitionCount);
    json.Jsonize("parallelNum", _processorBasicInfo.parallelNum, _processorBasicInfo.parallelNum);
    if (json.GetMode() == TO_JSON) {
        auto [mode, str] = serilize();
        json.Jsonize("serilize_mode", mode);
        json.Jsonize("processor_target_infos", str);
    } else {
        int32_t mode = 0;
        json.Jsonize("serilize_mode", mode, INVALID_MODE);
        if (mode != INVALID_MODE) {
            std::string infoStr;
            json.Jsonize("processor_target_infos", infoStr);
            deserilize(infoStr, mode);
        }
    }
}

std::pair<int32_t, std::string> ProcessorTargetInfos::serilize() const
{
    std::vector<std::string> infoVec;
    for (const auto& info : _processorTargetInfos) {
        infoVec.push_back(autil::legacy::ToJsonString(std::make_pair(info.offset, info.userData), true));
    }
    return {0, autil::legacy::ToJsonString(infoVec, true)};
}

void ProcessorTargetInfos::deserilize(const std::string& str, int32_t serilizeMode)
{
    if (serilizeMode == 0) {
        std::vector<std::string> infoVec;
        autil::legacy::FromJsonString(infoVec, str);
        for (const auto& infoStr : infoVec) {
            std::pair<int64_t, std::string> value;
            autil::legacy::FromJsonString(value, infoStr);
            _processorTargetInfos.emplace_back(value.first, value.second);
        }
        return;
    }
    AUTIL_LEGACY_THROW(autil::legacy::NotSupportException, "serilizeMode not support");
}
ProcessorTargetInfo ProcessorTargetInfos::get(size_t processorIdx) const
{
    assert(processorIdx < _processorTargetInfos.size());
    return _processorTargetInfos[processorIdx];
}
void ProcessorTargetInfos::update(const ProcessorBasicInfo& basicInfo,
                                  const std::vector<ProcessorTargetInfo>& targetInfos)
{
    // partition count or parallel num or src change
    if (_processorBasicInfo.checkSourceAndPartition(basicInfo)) {
        ProcessorTargetInfo targetInfo(basicInfo.offset, "");
        _processorBasicInfo = basicInfo;
        _processorTargetInfos.assign(targetInfos.size(), targetInfo);
    }
    assert(targetInfos.size() == _processorTargetInfos.size());
    for (size_t i = 0; i < targetInfos.size(); ++i) {
        if (targetInfos[i].offset > _processorTargetInfos[i].offset) {
            _processorTargetInfos[i] = targetInfos[i];
        }
    }
}

// void ProcessorTargetInfos::validOrClear(const ProcessorBasicInfo& basicInfo) { assert(false); }

}} // namespace build_service::admin
