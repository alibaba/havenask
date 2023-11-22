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

#include <functional>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "build_service/web_toolkit/base/HtmlTemplateRender.h"
#include "build_service/web_toolkit/base/TemplateDataAccessor.h"
#include "build_service/web_toolkit/third_party/inja/inja.hpp"
#include "indexlib/util/httplib.h"

namespace build_service::web_toolkit {

// maybe enable authentication by SSL or call other library with set_pre_routing_handler(jni?)
class SimpleHttpServer : private autil::NoCopyable
{
public:
    using Handler = std::function<void(const httplib::Request&, httplib::Response&)>;
    using Logger = std::function<void(const httplib::Request&, const httplib::Response&)>;
    using TemplateRenderPtr = std::shared_ptr<HtmlTemplateRender>;

public:
    SimpleHttpServer(int32_t threadNum = -1 /* -1 means use default 8 or hardware_concurrency() */,
                     const std::string& templateRootPath = "" /* template file's root path */,
                     const std::shared_ptr<TemplateDataAccessor>& templateDataAccessor = nullptr);
    ~SimpleHttpServer();

public:
    /* functions below should call before start() */
    // user can get Environment& to set_xxx before use Env object,
    // if do so, user should call before registerHtmlTemplateHandler
    inja::Environment& getInjaEnv() { return _env; }

    // static file resource
    bool registerMountPoint(const std::string& mountPoint, const std::string& dir);

    // user define handler
    bool registerRouteHandler(const std::string& routePath, Handler handler,
                              const std::vector<std::string>& methods = {});

    // render template file with data accessor
    bool registerHtmlTemplateHandler(const std::string& routePath, const std::string& templateFileRelPath,
                                     const std::vector<std::string>& methods = {});

    bool registerRedirectPath(const std::string& routePath, const std::string& redirectPath);

    void enableCacheControlForResourceFile();

    void setErrorHandler(Handler handler);
    void setLogger(Logger logger);

public:
    bool start(int32_t port = -1 /* -1 means use random port */);
    void stop();
    bool isRunning() const;
    int32_t getPort() const { return _port; }

private:
    bool insertRoutePath(const std::string& method, const std::string& routePath);
    bool loadTemplateFile(const std::string& templateFilePath);
    std::string getInternalErrorString(const std::string& summary, const std::string& detail);

private:
    httplib::Server _httpServer;
    inja::Environment _env;
    int32_t _port = -1;
    std::unordered_map<std::string, TemplateRenderPtr> _tmplRenderMap;
    std::shared_ptr<TemplateDataAccessor> _tmplDataAccessor;
    std::set<std::string> _routePathSet;
    std::shared_ptr<std::thread> _listenThread;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace build_service::web_toolkit
