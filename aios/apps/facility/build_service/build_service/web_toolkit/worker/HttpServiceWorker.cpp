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
#include "build_service/web_toolkit/worker/HttpServiceWorker.h"

#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"

namespace build_service::web_toolkit {
AUTIL_LOG_SETUP(build_service.web_toolkit, HttpServiceWorker);

class AccessLogger
{
public:
    static void log(const httplib::Request& req, const httplib::Response& res)
    {
        struct timeval tval;
        gettimeofday(&tval, nullptr);
        AUTIL_LOG(INFO,
                  "timestamp[%ld.%ld], remote_addr[%s:%d], "
                  "local_addr[%s:%d], method[%s], path[%s], "
                  "headers[%s], body[%s], params[%s], response status[%d], "
                  "response reason[%s], response headers[%s]",
                  tval.tv_sec, tval.tv_usec, req.remote_addr.c_str(), req.remote_port, req.local_addr.c_str(),
                  req.local_port, req.method.c_str(), req.path.c_str(), nlohmann::json(req.headers).dump().c_str(),
                  req.body.c_str(), nlohmann::json(req.params).dump().c_str(), res.status, res.reason.c_str(),
                  nlohmann::json(res.headers).dump().c_str());
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(build_service.web_toolkit, AccessLogger);

HttpServiceWorker::HttpServiceWorker(const std::shared_ptr<TemplateDataAccessorFactory>& factory) : _factory(factory) {}

HttpServiceWorker::~HttpServiceWorker() {}

bool HttpServiceWorker::init(const std::string& webRoot, const std::string& configPath,
                             RouteHandlerVector userDefineRouteHandles)
{
    std::string jsonStr;
    auto ec = fslib::fs::FileSystem::readFile(configPath, jsonStr);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "read config file [%s] fail.", configPath.c_str());
        return false;
    }

    try {
        auto obj = inja::json::parse(jsonStr.begin(), jsonStr.end());
        int32_t threadNum = -1;
        if (obj.contains("thread_number")) {
            threadNum = obj["thread_number"].get<int32_t>();
        }
        std::string tmplRoot = getTemplateDirPath(webRoot, obj);
        auto accessor = createDataAccessor(obj);
        _serverPtr.reset(new SimpleHttpServer(threadNum, tmplRoot, accessor));
        // register user-defined route handler
        for (auto& udfRouteHandler : userDefineRouteHandles) {
            if (!_serverPtr->registerRouteHandler(udfRouteHandler.first, udfRouteHandler.second)) {
                return false;
            }
        }
        if (!registerResourceDir(webRoot, obj) || !registerRouteTemplate(obj) || !registerRedirectPath(obj)) {
            return false;
        }
        int32_t port = -1;
        if (obj.contains("listen_port")) {
            port = obj["listen_port"].get<int32_t>();
        }
        if (obj.contains("enable_access_log")) {
            bool enableLogger = obj["enable_access_log"].get<bool>();
            if (enableLogger) {
                auto loggerFunc = [](const auto& req, const auto& res) { AccessLogger::log(req, res); };
                _serverPtr->setLogger(loggerFunc);
            }
        }
        _serverPtr->enableCacheControlForResourceFile(); // set max-age = 3600 for .js/.css/.png/.jpg  in web browner
        return _serverPtr->start(port);
    } catch (const nlohmann::detail::parse_error& e) {
        AUTIL_LOG(ERROR, "parse config file [%s] error: %s", configPath.c_str(), e.what());
        return false;
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "parse config file [%s] error: %s.", configPath.c_str(), e.what());
        return false;
    } catch (...) {
        AUTIL_LOG(ERROR, "parse config file [%s] error.", configPath.c_str());
        return false;
    }
    return false;
}

void HttpServiceWorker::stop()
{
    if (_serverPtr) {
        _serverPtr->stop();
    }
}

bool HttpServiceWorker::isRunning() const
{
    if (!_serverPtr) {
        return false;
    }
    return _serverPtr->isRunning();
}

int32_t HttpServiceWorker::getPort() const
{
    if (!_serverPtr) {
        return -1;
    }
    return _serverPtr->getPort();
}

std::shared_ptr<TemplateDataAccessor> HttpServiceWorker::createDataAccessor(const nlohmann::json& obj)
{
    std::shared_ptr<TemplateDataAccessor> accessor;
    if (!_factory) {
        return accessor;
    }

    std::string dataAccType;
    if (obj.contains("data_accessor_type")) {
        dataAccType = obj["data_accessor_type"].get<std::string>();
    }
    nlohmann::json accParam;
    if (obj.contains("data_accessor_parameter")) {
        accParam = obj["data_accessor_parameter"];
    }
    return _factory->createAccessor(dataAccType, accParam);
}

std::string HttpServiceWorker::getTemplateDirPath(const std::string& rootPath, const nlohmann::json& obj)
{
    std::string tmplDirName = "template";
    // register route template
    if (obj.contains("route_template_config")) {
        auto& tmplConfig = obj["route_template_config"];
        if (tmplConfig.contains("template_dir_name")) {
            tmplDirName = tmplConfig["template_dir_name"].get<std::string>();
        }
    }
    return fslib::util::FileUtil::joinPath(rootPath, tmplDirName);
}

bool HttpServiceWorker::registerResourceDir(const std::string& rootPath, const nlohmann::json& obj)
{
    if (obj.contains("resource_dir_names")) {
        // register resource dirs
        auto& resDirNames = obj["resource_dir_names"];
        for (size_t i = 0; i < resDirNames.size(); i++) {
            std::string resDir = resDirNames[i].get<std::string>();
            assert(*resDir.begin() != '/');
            std::string routeTarget = "/" + resDir;
            std::string targetDir = fslib::util::FileUtil::joinPath(rootPath, resDir);
            if (!_serverPtr->registerMountPoint(routeTarget, targetDir)) {
                return false;
            }
        }
    }

    if (obj.contains("mount_root_target_dir")) {
        std::string dir = obj["mount_root_target_dir"].get<std::string>();
        std::string targetDir = fslib::util::FileUtil::joinPath(rootPath, dir);
        if (!_serverPtr->registerMountPoint("/", targetDir)) {
            return false;
        }
    }
    return true;
}

bool HttpServiceWorker::registerRouteTemplate(const nlohmann::json& obj)
{
    if (!obj.contains("route_template_config")) {
        return true;
    }
    // TODO: parse inja env paramters
    auto& tmplConfig = obj["route_template_config"];
    if (tmplConfig.contains("route_rules")) {
        auto& routeTmplInfos = tmplConfig["route_rules"];
        for (size_t i = 0; i < routeTmplInfos.size(); i++) {
            auto& routeTmplInfo = routeTmplInfos[i];
            if (!routeTmplInfo.contains("path") || !routeTmplInfo.contains("file")) {
                AUTIL_LOG(ERROR, "[path] or [file] not exist in route_template_config.");
                return false;
            }
            if (!_serverPtr->registerHtmlTemplateHandler(routeTmplInfo["path"].get<std::string>(),
                                                         routeTmplInfo["file"].get<std::string>())) {
                return false;
            }
        }
    }
    return true;
}

bool HttpServiceWorker::registerRedirectPath(const nlohmann::json& obj)
{
    if (!obj.contains("redirect_path_config")) {
        return true;
    }
    auto& redirectConfigs = obj["redirect_path_config"];
    for (size_t i = 0; i < redirectConfigs.size(); i++) {
        auto& redirectConfig = redirectConfigs[i];
        if (!redirectConfig.contains("from") || !redirectConfig.contains("to")) {
            AUTIL_LOG(ERROR, "[from] or [to] not exist in redirect_path_config.");
            return false;
        }
        if (!_serverPtr->registerRedirectPath(redirectConfig["from"].get<std::string>(),
                                              redirectConfig["to"].get<std::string>())) {
            return false;
        }
    }
    return true;
}

} // namespace build_service::web_toolkit
