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

#include <mutex>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "autil/NoCopyable.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/ITabletPortal.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::framework {

class TabletPortalManager : public indexlib::util::Singleton<TabletPortalManager>
{
public:
    TabletPortalManager();
    ~TabletPortalManager();

    /*
      工作目录下，默认读取[tablet_portal.json]文件加载tablet_portal, 内部格式KV map，配置文件路径环境变量可以配置
      配置文件格式:
      [
         {
            "type" : "http",
            "port" : "8888",
            "thread_num" : "3",
            "address_file" : "http_port.info"
         },
         ....
     ]
    */
    std::string GetConfigFilePath() const;
    void GetDefaultParam(indexlib::util::KeyValueMap& param);
    bool IsActivated() const { return _isInited; }

private:
    void Init();
    std::shared_ptr<ITabletPortal> CreateNewPortal(const indexlib::util::KeyValueMap& param);
    void WorkLoop();

private:
    static const int64_t RELOAD_LOOP_INTERVAL = 5 * 1000 * 1000; /* 5s */

    volatile bool _isInited = false;
    int64_t _lastTs = -1;
    std::string _configPath;
    fslib::PathMeta _configPathMeta;
    std::map<std::string, std::shared_ptr<ITabletPortal>> _tabletPortals;
    std::mutex _mutex;
    autil::LoopThreadPtr _loopThread;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
