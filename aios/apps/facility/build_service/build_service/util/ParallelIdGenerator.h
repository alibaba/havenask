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

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/IdGenerator.h"

namespace build_service::util {

class ParallelIdGenerator : public indexlibv2::framework::IdGenerator
{
public:
    explicit ParallelIdGenerator(indexlibv2::IdMaskType maskType);
    ParallelIdGenerator(indexlibv2::IdMaskType segMaskType, indexlibv2::IdMaskType versionMaskType);

    indexlib::Status Init(int32_t instanceId, int32_t parallelNum);

private:
    indexlibv2::segmentid_t GetNextSegmentIdUnSafe() const override;
    indexlibv2::segmentid_t GetNextVersionIdUnSafe() const override;
    int32_t CalculateInstanceIdBySegId(indexlibv2::segmentid_t segId) const;
    int32_t CalculateInstanceIdByVersionId(indexlibv2::segmentid_t segId) const;

private:
    int32_t _instanceId = 0;
    int32_t _parallelNum = 1;

    AUTIL_LOG_DECLARE();
};

} // namespace build_service::util
