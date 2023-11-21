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

#include <string>
#include <vector>

#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {

class AlterFieldCKPAccessor
{
public:
    AlterFieldCKPAccessor();
    ~AlterFieldCKPAccessor();

private:
    AlterFieldCKPAccessor(const AlterFieldCKPAccessor&);
    AlterFieldCKPAccessor& operator=(const AlterFieldCKPAccessor&);

public:
    static bool getOngoingOpIds(const common::CheckpointAccessorPtr& checkpointAccessor, const std::string& clusterName,
                                schema_opid_t maxOpId, std::vector<schema_opid_t>& ongoingOpIds);
    static void updateCheckpoint(const std::string& clusterName, schema_opid_t opsId,
                                 common::CheckpointAccessorPtr checkpointAccessor);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AlterFieldCKPAccessor);

}} // namespace build_service::admin
