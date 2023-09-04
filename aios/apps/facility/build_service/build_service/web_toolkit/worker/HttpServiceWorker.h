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
#include "build_service/web_toolkit/base/SimpleHttpServer.h"
#include "build_service/web_toolkit/worker/TemplateDataAccessorFactory.h"

namespace build_service::web_toolkit {

// TODO: support buc acl & session
class HttpServiceWorker : private autil::NoCopyable
{
public:
    using Handler = SimpleHttpServer::Handler;
    using RouteHandler = std::pair<std::string, Handler>;
    using RouteHandlerVector = std::vector<RouteHandler>;

public:
    HttpServiceWorker(const std::shared_ptr<TemplateDataAccessorFactory>& factory);
    ~HttpServiceWorker();

public:
    /*
      rootPath:
      / config.json
      / template / t1.html, t2.html
      / pages
      / plugin
      / data
    */
    bool init(const std::string& webRoot, const std::string& configPath, RouteHandlerVector userDefineRouteHandles);

    void stop();
    bool isRunning() const;
    int32_t getPort() const;

private:
    std::shared_ptr<TemplateDataAccessor> createDataAccessor(const nlohmann::json& obj);
    std::string getTemplateDirPath(const std::string& rootPath, const nlohmann::json& obj);
    bool registerResourceDir(const std::string& rootPath, const nlohmann::json& obj);
    bool registerRouteTemplate(const nlohmann::json& obj);
    bool registerRedirectPath(const nlohmann::json& obj);

private:
    std::shared_ptr<SimpleHttpServer> _serverPtr;
    std::shared_ptr<TemplateDataAccessorFactory> _factory;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace build_service::web_toolkit
