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
#include "build_service/util/ParallelIdGenerator.h"

#include "indexlib/base/Constant.h"
#include "indexlib/framework/Segment.h"

namespace build_service::util {
AUTIL_LOG_SETUP(build_service.util, ParallelIdGenerator);

ParallelIdGenerator::ParallelIdGenerator(indexlibv2::IdMaskType maskType) : indexlibv2::framework::IdGenerator(maskType)
{
}

ParallelIdGenerator::ParallelIdGenerator(indexlibv2::IdMaskType segMaskType, indexlibv2::IdMaskType versionMaskType)
    : indexlibv2::framework::IdGenerator(segMaskType, versionMaskType)
{
}

indexlib::Status ParallelIdGenerator::Init(int32_t instanceId, int32_t parallelNum)
{
    if (instanceId >= 0 && parallelNum > 0 && parallelNum > instanceId) {
        _instanceId = instanceId;
        _parallelNum = parallelNum;
        AUTIL_LOG(INFO, "init ParallelIdGenerator with instanceId[%d] parallelNum[%d]", instanceId, parallelNum)
        return indexlib::Status::OK();
    }
    AUTIL_LOG(ERROR, "init failed. invalid instanceId[%d] parallelNum[%d]", instanceId, parallelNum);
    return indexlib::Status::InvalidArgs();
}

int32_t ParallelIdGenerator::CalculateInstanceIdBySegId(indexlibv2::segmentid_t segId) const
{
    if (segId & GetSegmentIdMask()) {
        return (segId ^ GetSegmentIdMask()) % _parallelNum;
    }
    return -1;
}

int32_t ParallelIdGenerator::CalculateInstanceIdByVersionId(indexlibv2::versionid_t versionId) const
{
    if (versionId & GetVersionIdMask()) {
        return (versionId ^ GetVersionIdMask()) % _parallelNum;
    }
    return -1;
}

indexlibv2::segmentid_t ParallelIdGenerator::GetNextSegmentIdUnSafe() const
{
    if (_currentSegmentId == indexlibv2::INVALID_SEGMENTID) {
        return GetSegmentIdMask() | _instanceId;
    }
    int32_t instanceId = CalculateInstanceIdBySegId(_currentSegmentId);
    assert(instanceId >= 0);
    int32_t offset = _instanceId - instanceId;
    offset = (offset > 0) ? offset : offset + _parallelNum;
    return _currentSegmentId + offset;
}

indexlibv2::segmentid_t ParallelIdGenerator::GetNextVersionIdUnSafe() const
{
    if (_currentVersionId == indexlibv2::INVALID_VERSIONID) {
        return GetVersionIdMask() | _instanceId;
    }
    int32_t instanceId = CalculateInstanceIdByVersionId(_currentVersionId);
    assert(instanceId >= 0);
    int32_t offset = _instanceId - instanceId;
    offset = (offset > 0) ? offset : offset + _parallelNum;
    return _currentVersionId + offset;
}

} // namespace build_service::util
