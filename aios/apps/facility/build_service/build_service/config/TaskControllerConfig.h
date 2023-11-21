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

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class TaskControllerConfig : public autil::legacy::Jsonizable
{
public:
    TaskControllerConfig();
    ~TaskControllerConfig();

public:
    enum Type { CT_BUILDIN, CT_CUSTOM, CT_UNKOWN };

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    Type getControllerType() const { return _type; }
    const std::vector<TaskTarget>& getTargetConfigs() const { return _targets; }

private:
    std::string typeToStr();
    Type strToType(const std::string& str);

private:
    Type _type;
    std::vector<TaskTarget> _targets;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskControllerConfig);

}} // namespace build_service::config
