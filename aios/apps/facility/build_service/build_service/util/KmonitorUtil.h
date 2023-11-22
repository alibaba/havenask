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

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "kmonitor/client/core/MetricsTags.h"

namespace build_service { namespace util {

class KmonitorUtil
{
public:
    static void getTags(const proto::PartitionId& pid, kmonitor::MetricsTags& tags);
    static void getTags(const proto::Range& range, const std::string& clusterName, const std::string& dataTable,
                        generationid_t generationId, kmonitor::MetricsTags& tags);
    static bool validateAndAddTag(const std::string& tagk, const std::string& tagv, kmonitor::MetricsTags& tags);

private:
    static std::string getApp();

private:
    BS_LOG_DECLARE();
};
}} // namespace build_service::util
