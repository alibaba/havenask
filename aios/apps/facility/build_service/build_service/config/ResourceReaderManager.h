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

#include <atomic>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"
#include "indexlib/util/Singleton.h"

namespace build_service { namespace config {

class ResourceReaderManager : public indexlib::util::Singleton<ResourceReaderManager>
{
public:
    ResourceReaderManager();
    ~ResourceReaderManager();

private:
    ResourceReaderManager(const ResourceReaderManager&);
    ResourceReaderManager& operator=(const ResourceReaderManager&);

public:
    static ResourceReaderPtr getResourceReader(const std::string& configPath);

private:
    void clearLoop();

private:
    std::unordered_map<std::string, std::pair<ResourceReaderPtr, int64_t>> _readerCache;
    mutable autil::ThreadMutex _cacheMapLock;
    bool _enableCache;
    std::atomic<bool> _close;
    int64_t _clearLoopInterval;
    int64_t _cacheExpireTime;
    autil::LoopThreadPtr _loopThread;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ResourceReaderManager);

}} // namespace build_service::config
