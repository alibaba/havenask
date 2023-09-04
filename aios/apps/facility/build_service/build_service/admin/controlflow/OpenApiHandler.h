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
#ifndef ISEARCH_BS_OPENAPIHANDLER_H
#define ISEARCH_BS_OPENAPIHANDLER_H

#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class OpenApiHandler
{
public:
    class CmdPathParser
    {
    public:
        enum TargetType { UNKNOWN, RESOURCE };

    public:
        void parse(const std::string& path);
        TargetType getType() { return _type; }
        std::string getResourceType() { return _resourceType; }

    private:
        TargetType _type;
        std::string _resourceType;
    };

public:
    OpenApiHandler(const TaskResourceManagerPtr& taskResMgr);
    ~OpenApiHandler();

private:
    OpenApiHandler(const OpenApiHandler&);
    OpenApiHandler& operator=(const OpenApiHandler&);

public:
    bool handle(const std::string& cmd, const std::string& path, const KeyValueMap& params);

private:
    TaskResourceManagerPtr _taskResourceManager;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(OpenApiHandler);

}} // namespace build_service::admin

#endif // ISEARCH_BS_OPENAPIHANDLER_H
