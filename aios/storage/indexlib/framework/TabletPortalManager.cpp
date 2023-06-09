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
#include "indexlib/framework/TabletPortalManager.h"

#include "autil/EnvUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/framework/HttpTabletPortal.h"
#include "indexlib/framework/TabletCenter.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletPortalManager);

TabletPortalManager::TabletPortalManager() { Init(); }

TabletPortalManager::~TabletPortalManager() {}

void TabletPortalManager::Init()
{
    if (!autil::EnvUtil::getEnv("INDEXLIB_ENABLE_TABLET_PORTAL", false)) {
        return;
    }

    indexlib::util::KeyValueMap defaultParam;
    GetDefaultParam(defaultParam);
    auto defaultPortal = CreateNewPortal(defaultParam);
    if (defaultPortal) {
        std::string paramStr = autil::legacy::ToJsonString(defaultParam, true);
        _tabletPortals[paramStr] = defaultPortal;
    }

    _configPath = GetConfigFilePath();
    _configPathMeta = fslib::PathMeta();
    _loopThread =
        autil::LoopThread::createLoopThread(std::bind(&TabletPortalManager::WorkLoop, this), 10 * 1000, /* 10ms */
                                            "TabletPortalMgr");
    _isInited = true;
}

std::shared_ptr<ITabletPortal> TabletPortalManager::CreateNewPortal(const indexlib::util::KeyValueMap& param)
{
    if (param.empty()) {
        return std::shared_ptr<ITabletPortal>();
    }

    auto type = indexlib::util::GetValueFromKeyValueMap(param, "type");
    if (type == "http") {
        int32_t threadNum = indexlib::util::GetTypeValueFromKeyValueMap(param, "thread_num", (int32_t)2);
        auto ret = std::make_shared<HttpTabletPortal>(threadNum);
        if (!ret->Start(param)) {
            ret.reset();
        }
        return ret;
    }

    AUTIL_LOG(ERROR, "create tablet portal fail, unknown type [%s].", type.c_str());
    return std::shared_ptr<ITabletPortal>();
}

std::string TabletPortalManager::GetConfigFilePath() const
{
    std::string path;
    path = autil::EnvUtil::getEnv("INDEXLIB_TABLET_PORTAL_CONFIG_PATH");
    if (!path.empty()) {
        return path;
    }
    char cwdPath[PATH_MAX];
    char* ret = getcwd(cwdPath, PATH_MAX);
    if (ret) {
        path = std::string(cwdPath);
        if ('/' != *(path.rbegin())) {
            path += "/";
        }
        path += "tablet_portal.json";
    }
    return path;
}

void TabletPortalManager::WorkLoop()
{
    int64_t currentTs = autil::TimeUtility::currentTime();
    if (currentTs - _lastTs < RELOAD_LOOP_INTERVAL) {
        return;
    }

    _lastTs = currentTs;
    std::lock_guard<std::mutex> guard(_mutex);
    if (_configPath.empty()) {
        return;
    }

    auto ret = indexlib::file_system::FslibWrapper::GetPathMeta(_configPath);
    if (!ret.OK() || !ret.result.isFile) {
        return;
    }

    if (ret.result.length == _configPathMeta.length && ret.result.createTime == _configPathMeta.createTime &&
        ret.result.lastModifyTime == _configPathMeta.lastModifyTime) {
        // path meta no change
        for (auto it = _tabletPortals.begin(); it != _tabletPortals.end();) {
            if (it->second->IsRunning()) {
                it++;
                continue;
            }
            AUTIL_LOG(INFO, "remove tablet portal which is not running, param [%s]", it->first.c_str());
            _tabletPortals.erase(it++);
        }
        return;
    }

    std::string content;
    if (!indexlib::file_system::FslibWrapper::AtomicLoad(_configPath, content).OK()) {
        AUTIL_LOG(ERROR, "load tablet portal config [%s] fail.", _configPath.c_str());
        return;
    }

    std::vector<indexlib::util::KeyValueMap> params;
    try {
        autil::legacy::FromJsonString(params, content);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "load tablet portal config [%s] fail, invalid json string: %s, exception [%s]",
                  _configPath.c_str(), content.c_str(), e.what());
        _configPathMeta = ret.result;
        return;
    }

    std::map<std::string, std::shared_ptr<ITabletPortal>> newPortalMap;
    std::vector<indexlib::util::KeyValueMap> newParamVec;
    for (auto& param : params) {
        std::string paramStr = autil::legacy::ToJsonString(param, true);
        auto iter = _tabletPortals.find(paramStr);
        if (iter != _tabletPortals.end() && iter->second->IsRunning()) {
            newPortalMap[paramStr] = iter->second;
        } else {
            newParamVec.push_back(param);
        }
    }

    _tabletPortals.clear();
    for (auto& param : newParamVec) {
        std::string paramStr = autil::legacy::ToJsonString(param, true);
        auto newPortal = CreateNewPortal(param);
        if (!newPortal) {
            AUTIL_LOG(ERROR, "create new tablet portal from param [%s] fail.", paramStr.c_str());
            continue;
        }
        newPortalMap[paramStr] = newPortal;
    }
    newPortalMap.swap(_tabletPortals);
    if (_tabletPortals.size() == params.size()) {
        _configPathMeta = ret.result;
    }
}

void TabletPortalManager::GetDefaultParam(indexlib::util::KeyValueMap& param)
{
    std::string defaultParam = autil::EnvUtil::getEnv("INDEXLIB_TABLET_PORTAL_DEFAULT_PARAM", "");
    std::vector<std::vector<std::string>> paramVec;
    autil::StringUtil::fromString(defaultParam, paramVec, "=", ";");
    for (auto& p : paramVec) {
        if (p.size() == 2) {
            param[p[0]] = p[1];
        }
    }
}

__attribute__((constructor)) void InitializeTabletPortalManager()
{
    auto mgr = indexlibv2::framework::TabletPortalManager::GetInstance();
    if (!mgr->IsActivated()) {
        return;
    }

    std::cout << "initialize tablet portal manager with config file path:" << mgr->GetConfigFilePath() << std::endl;

    indexlib::util::KeyValueMap param;
    mgr->GetDefaultParam(param);
    std::cout << "initialize tablet portal manager with default parameter from ENV:"
              << autil::legacy::ToJsonString(param, true) << std::endl;

    std::cout << "initialize tablet portal manager with tablet center activated." << std::endl;
    indexlibv2::framework::TabletCenter::GetInstance()->Activate();
}

} // namespace indexlibv2::framework
