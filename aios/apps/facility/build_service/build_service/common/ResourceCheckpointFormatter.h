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

#include "autil/StringUtil.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace common {

class ResourceCheckpointFormatter
{
public:
    ResourceCheckpointFormatter();
    ~ResourceCheckpointFormatter();

private:
    ResourceCheckpointFormatter(const ResourceCheckpointFormatter&);
    ResourceCheckpointFormatter& operator=(const ResourceCheckpointFormatter&);

public:
    static std::string getResourceCheckpointId(const std::string& resourceType)
    {
        return BS_RESOURCE_CHECKPOINT_ID + "_" + resourceType;
    }
    static std::string encodeResourceCheckpointName(versionid_t resourceVersion)
    {
        return autil::StringUtil::toString(resourceVersion);
    }

    static versionid_t decodeResourceCheckpointName(const std::string& idStr)
    {
        versionid_t ret = indexlib::INVALID_VERSIONID;
        autil::StringUtil::fromString(idStr, ret);
        return ret;
    }
};

}} // namespace build_service::common
