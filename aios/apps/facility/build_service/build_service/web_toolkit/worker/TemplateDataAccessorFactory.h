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
#include "autil/NoCopyable.h"
#include "build_service/web_toolkit/third_party/nlohmann/json.hpp"

namespace build_service::web_toolkit {

class TemplateDataAccessor;

class TemplateDataAccessorFactory : private autil::NoCopyable
{
public:
    TemplateDataAccessorFactory() {}
    virtual ~TemplateDataAccessorFactory() {}

public:
    virtual std::shared_ptr<TemplateDataAccessor> createAccessor(const std::string& type, nlohmann::json params) = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace build_service::web_toolkit
