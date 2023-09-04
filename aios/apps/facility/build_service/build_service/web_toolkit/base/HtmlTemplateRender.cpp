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
#include "build_service/web_toolkit/base/HtmlTemplateRender.h"

namespace build_service::web_toolkit {
AUTIL_LOG_SETUP(build_service.web_toolkit, HtmlTemplateRender);

HtmlTemplateRender::HtmlTemplateRender(inja::Environment& env) : _env(env) {}

HtmlTemplateRender::~HtmlTemplateRender() {}

bool HtmlTemplateRender::init(const std::string& tmplFile, const std::string& contentType)
{
    try {
        _tmpl = _env.parse_file(tmplFile);
    } catch (const std::runtime_error& e) {
        AUTIL_LOG(ERROR, "parse file [%s] error: %s", tmplFile.c_str(), e.what());
        return false;
    } catch (...) {
        AUTIL_LOG(ERROR, "unknown error when parse template file [%s]", tmplFile.c_str());
        return false;
    }
    _contentType = contentType;
    return true;
}

std::string HtmlTemplateRender::render(const std::shared_ptr<TemplateData>& tmplData)
{
    assert(tmplData);
    auto result = tmplData->toString();
    if (!result.is_ok()) {
        throw std::runtime_error(std::string("[TemplateData.toString()] error: ") + result.get_error().message());
    }
    auto data = result.get();
    auto dataObj = inja::json::parse(data.begin(), data.end());
    return _env.render(_tmpl, dataObj);
}

} // namespace build_service::web_toolkit
