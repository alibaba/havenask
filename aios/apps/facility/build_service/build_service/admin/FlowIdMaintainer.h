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
#ifndef ISEARCH_BS_FLOWIDMAINTAINER_H
#define ISEARCH_BS_FLOWIDMAINTAINER_H

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/controlflow/TaskFlowManager.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class FlowIdMaintainer : public autil::legacy::Jsonizable
{
public:
    FlowIdMaintainer();
    ~FlowIdMaintainer();

private:
    FlowIdMaintainer(const FlowIdMaintainer&);
    FlowIdMaintainer& operator=(const FlowIdMaintainer&);

public:
    void cleanFlows(const TaskFlowManagerPtr& manager, const std::string& generationDir, bool keepRelativeFlow = false,
                    bool keepRecentFlow = false);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    void clearWorkerZkNode(const TaskFlowPtr& flow, const std::string& generationDir);

private:
    int64_t _clearInterval;
    std::map<std::string, int64_t> _stopFlowInfos;
    static int64_t DEFAULT_CLEAR_INTERVAL; // second

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FlowIdMaintainer);

}} // namespace build_service::admin

#endif // ISEARCH_BS_FLOWIDMAINTAINER_H
