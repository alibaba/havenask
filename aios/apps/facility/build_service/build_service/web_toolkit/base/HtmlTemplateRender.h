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

#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "build_service/web_toolkit/base/TemplateDataAccessor.h"
#include "build_service/web_toolkit/third_party/inja/inja.hpp"

namespace build_service::web_toolkit {

class HtmlTemplateRender : private autil::NoCopyable
{
public:
    HtmlTemplateRender(inja::Environment& env);
    ~HtmlTemplateRender();

public:
    bool init(const std::string& tmplFile, const std::string& contentType);
    std::string render(const std::shared_ptr<TemplateData>& data);
    std::string getContentType() const { return _contentType; }

private:
    inja::Environment& _env;
    inja::Template _tmpl;
    std::string _contentType;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace build_service::web_toolkit
