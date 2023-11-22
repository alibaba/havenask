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
#include "SimpleHttpServer.h"

#include <algorithm>
#include <assert.h>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <stdio.h>
#include <utility>

#include "autil/result/Result.h"
#include "build_service/web_toolkit/third_party/nlohmann/json.hpp"
#include "fslib/util/FileUtil.h"

namespace build_service::web_toolkit {
AUTIL_LOG_SETUP(build_service.web_toolkit, SimpleHttpServer);

SimpleHttpServer::SimpleHttpServer(int32_t threadNum, const std::string& tmplRoot,
                                   const std::shared_ptr<TemplateDataAccessor>& templateDataAccessor)
    : _env(fslib::util::FileUtil::normalizeDir(tmplRoot))
    , _tmplDataAccessor(templateDataAccessor)
{
    if (threadNum > 0) {
        _httpServer.new_task_queue = [threadNum] { return new httplib::ThreadPool(threadNum); };
    }
    _httpServer.set_exception_handler([](const auto& req, auto& res, std::exception_ptr ep) {
        auto fmt = "<h1>Error 500</h1><p>%s</p>";
        char buf[BUFSIZ];
        try {
            std::rethrow_exception(ep);
        } catch (std::exception& e) {
            snprintf(buf, sizeof(buf), fmt, e.what());
        } catch (...) { // See the following NOTE
            snprintf(buf, sizeof(buf), fmt, "Unknown Exception");
        }
        res.set_content(buf, "text/html");
        res.status = 500;
    });
}

SimpleHttpServer::~SimpleHttpServer()
{
    if (isRunning()) {
        stop();
    }
    if (_listenThread) {
        _listenThread->join();
    }
}

/* static file resource */
bool SimpleHttpServer::registerMountPoint(const std::string& mountPoint, const std::string& dir)
{
    if (isRunning()) {
        AUTIL_LOG(ERROR, "cannot registerMountPoint for running http server");
        return false;
    }
    auto ret = _httpServer.set_mount_point(mountPoint, dir);
    if (ret) {
        AUTIL_LOG(INFO, "registerMountPoint [%s] to dir [%s] success.", mountPoint.c_str(), dir.c_str());
    } else {
        AUTIL_LOG(ERROR, "registerMountPoint [%s] to dir [%s] fail.", mountPoint.c_str(), dir.c_str());
    }
    return ret;
}

/* user define handler */
bool SimpleHttpServer::registerRouteHandler(const std::string& routePath, Handler handler,
                                            const std::vector<std::string>& methods)
{
    if (isRunning()) {
        AUTIL_LOG(ERROR, "cannot registerRouteHandler for running http server");
        return false;
    }

    std::vector<std::string> targetMethods(methods.begin(), methods.end());
    if (targetMethods.empty()) {
        targetMethods.push_back("post");
        targetMethods.push_back("get");
    }
    for (const auto& method : targetMethods) {
        if (!insertRoutePath(method, routePath)) {
            return false;
        }
        AUTIL_LOG(INFO, "register routePath [%s] for method [%s].", routePath.c_str(), method.c_str());
        if (method == "post") {
            _httpServer.Post(routePath, handler);
            continue;
        }
        if (method == "get") {
            _httpServer.Get(routePath, handler);
            continue;
        }
        AUTIL_LOG(ERROR, "unsupported method [%s] for routePath [%s]", method.c_str(), routePath.c_str());
        return false;
    }
    return true;
}

/* render template file with data accessor */
bool SimpleHttpServer::registerHtmlTemplateHandler(const std::string& routePath, const std::string& templateFilePath,
                                                   const std::vector<std::string>& methods)
{
    if (!_tmplDataAccessor) {
        AUTIL_LOG(ERROR, "cannot registerHtmlTemplateHandler without templateDataAccessor!");
        return false;
    }

    if (isRunning()) {
        AUTIL_LOG(ERROR, "cannot registerHtmlTemplateHandler for running http server!");
        return false;
    }

    if (!templateFilePath.empty() && _tmplRenderMap.find(templateFilePath) == _tmplRenderMap.end() &&
        !loadTemplateFile(templateFilePath)) {
        AUTIL_LOG(ERROR, "load template file [%s] fail.", templateFilePath.c_str());
        return false;
    }

    std::vector<std::string> targetMethods(methods.begin(), methods.end());
    if (targetMethods.empty()) {
        targetMethods.push_back("post");
        targetMethods.push_back("get");
    }
    for (const auto& method : targetMethods) {
        if (!insertRoutePath(method, routePath)) {
            return false;
        }
        AUTIL_LOG(INFO, "register routePath [%s] for method [%s], with template file [%s].", routePath.c_str(),
                  method.c_str(), templateFilePath.c_str());
        auto renderFunc = [this, targetRoutePath = routePath, filePath = templateFilePath](const httplib::Request& req,
                                                                                           httplib::Response& res) {
            assert(_tmplDataAccessor);
            auto tmplData = _tmplDataAccessor->extractData(targetRoutePath, req);
            if (tmplData == nullptr) {
                res.set_content(getInternalErrorString("extractData from TemplateDataAccessor fail!",
                                                       "templateData object is nullptr!"),
                                "text/html");
                return;
            }
            auto iter = _tmplRenderMap.find(filePath);
            if (iter == _tmplRenderMap.end()) {
                auto result = tmplData->toString();
                if (!result.is_ok()) {
                    throw std::runtime_error(std::string("[TemplateData.toString()] error: ") +
                                             result.get_error().message());
                }
                res.set_content(result.get(), "text/json");
                return;
            }
            auto& tmplRender = iter->second;
            try {
                res.set_content(tmplRender->render(tmplData), tmplRender->getContentType());
            } catch (const inja::RenderError& e) {
                std::string summary = "render template file [" + filePath + "] error!";
                res.set_content(getInternalErrorString(summary, e.what()), "text/html");
            } catch (const nlohmann::detail::parse_error& e) {
                res.set_content(getInternalErrorString("parse json data error!", e.what()), "text/html");
            } catch (...) {
                throw;
            }
        };
        if (method == "post") {
            _httpServer.Post(routePath, renderFunc);
            continue;
        }
        if (method == "get") {
            _httpServer.Get(routePath, renderFunc);
            continue;
        }
        AUTIL_LOG(ERROR, "unsupported method [%s] for routePath [%s]", method.c_str(), routePath.c_str());
        return false;
    }
    return true;
}

bool SimpleHttpServer::registerRedirectPath(const std::string& routePath, const std::string& redirectPath)
{
    if (isRunning()) {
        AUTIL_LOG(ERROR, "cannot registerRedirectPath for running http server");
        return false;
    }
    if (!insertRoutePath("post", routePath) || !insertRoutePath("get", routePath)) {
        return false;
    }
    AUTIL_LOG(INFO, "register routePath [%s], redirect to path [%s]", routePath.c_str(), redirectPath.c_str());
    auto redirectFunc = [url = redirectPath](const httplib::Request& req, httplib::Response& res) {
        res.set_redirect(url);
    };
    _httpServer.Post(routePath, redirectFunc);
    _httpServer.Get(routePath, redirectFunc);
    return true;
}

void SimpleHttpServer::setErrorHandler(Handler handler) { _httpServer.set_error_handler(handler); }

void SimpleHttpServer::setLogger(Logger logger) { _httpServer.set_logger(logger); }

bool SimpleHttpServer::start(int32_t port)
{
    if (port > 0 && !_httpServer.bind_to_port("0.0.0.0", port)) {
        AUTIL_LOG(ERROR, "SimpleHttpServer bind to port: [%d] fail.", port);
        return false;
    }

    if (port <= 0) {
        _port = _httpServer.bind_to_any_port("0.0.0.0"); // use random port
    } else {
        _port = port;
    }
    AUTIL_LOG(INFO, "SimpleHttpServer will listen to port: [%d]", _port);
    _listenThread.reset(new std::thread([this]() {
        if (!_httpServer.listen_after_bind()) {
            std::cerr << "SimpleHttpServer call listen_after_bind() fail." << std::endl;
        }
        std::cerr << "SimpleHttpServer is not running" << std::endl;
    }));
    return true;
}

void SimpleHttpServer::stop()
{
    AUTIL_LOG(INFO, "SimpleHttpServer will stop running.");
    _httpServer.stop();
}

bool SimpleHttpServer::isRunning() const { return _httpServer.is_running(); }

bool SimpleHttpServer::insertRoutePath(const std::string& method, const std::string& routePath)
{
    if (routePath.find("/") != 0) {
        AUTIL_LOG(ERROR, "invalid routePath [%s], which should begin with \"/\"", routePath.c_str());
        return false;
    }
    std::string newTargetRoute = method + ":" + routePath;
    if (_routePathSet.find(newTargetRoute) != _routePathSet.end()) {
        AUTIL_LOG(ERROR, "duplicated route path [%s], method [%s]", routePath.c_str(), method.c_str());
        return false;
    }
    _routePathSet.insert(newTargetRoute);
    return true;
}

bool SimpleHttpServer::loadTemplateFile(const std::string& templateFilePath)
{
    auto render = std::make_shared<HtmlTemplateRender>(_env);
    if (!render->init(templateFilePath, "text/html;charset=utf-8")) {
        return false;
    }
    _tmplRenderMap[templateFilePath] = render;
    return true;
}

std::string SimpleHttpServer::getInternalErrorString(const std::string& summary, const std::string& detail)
{
    auto fmt = "<h1>Error 500</h1><h2>summary</h2><p>%s</p><h2>detail</h2><p>%s</p>";
    char buf[BUFSIZ];
    snprintf(buf, sizeof(buf), fmt, summary.c_str(), detail.c_str());
    return std::string(buf);
}

void SimpleHttpServer::enableCacheControlForResourceFile()
{
    _httpServer.set_post_routing_handler([](const auto& req, auto& res) {
        auto pos = req.path.rfind(".");
        if (pos != std::string::npos) {
            std::string suffix = req.path.substr(pos + 1);
            if (suffix == "js" || suffix == "css" || suffix == "png" || suffix == "jpg") {
                res.set_header("Cache-Control", "max-age=3600");
            }
        }
    });
}

} // namespace build_service::web_toolkit
