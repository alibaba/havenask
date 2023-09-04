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
#include "autil/result/Result.h"
#include "indexlib/util/httplib.h"

namespace build_service::web_toolkit {

class TemplateData
{
public:
    TemplateData() {}
    virtual ~TemplateData() {}

public:
    /* return json string */
    virtual autil::Result<std::string> toString() = 0;
};

class DefaultTemplateData : public TemplateData
{
public:
    DefaultTemplateData(const std::string& str) : _str(str) {}

public:
    /* return json string */
    autil::Result<std::string> toString() override { return autil::Result<std::string>(_str); }

private:
    std::string _str;
};

///////////////////////////////////////////////////////////////
class TemplateDataAccessor : private autil::NoCopyable
{
public:
    TemplateDataAccessor() {}
    virtual ~TemplateDataAccessor() {}

public:
    virtual std::shared_ptr<TemplateData> extractData(const std::string& routePath,
                                                      const httplib::Request& req) const = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace build_service::web_toolkit
