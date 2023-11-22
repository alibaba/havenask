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
#include "indexlib/file_system/load_config/LoadConfigList.h"

#include <set>

#include "autil/StringUtil.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LoadConfigList);

struct LoadConfigList::Impl {
    LoadConfigVector loadConfigs;
    LoadConfig defaultLoadConfig;
    size_t totalCacheMemorySize = 0;
    std::shared_ptr<bool> enableLoadSpeedLimit;
};

LoadConfigList::LoadConfigList() : _impl(std::make_unique<LoadConfigList::Impl>())
{
    LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");
    _impl->defaultLoadConfig.SetFilePatternString(pattern);
    _impl->enableLoadSpeedLimit = std::make_shared<bool>(true);
}
LoadConfigList::LoadConfigList(const LoadConfigList& other)
    : _impl(std::make_unique<LoadConfigList::Impl>(*other._impl))
{
}
LoadConfigList& LoadConfigList::operator=(const LoadConfigList& other)
{
    if (this != &other) {
        _impl = std::make_unique<LoadConfigList::Impl>(*other._impl);
    }
    return *this;
}
LoadConfigList::~LoadConfigList() {}

static size_t GetMemorySize(const LoadConfig& loadConfig)
{
    const LoadStrategyPtr& strategy = loadConfig.GetLoadStrategy();
    if (strategy->GetLoadStrategyName() != READ_MODE_CACHE) {
        return 0;
    }
    CacheLoadStrategyPtr cacheStrategy = std::dynamic_pointer_cast<CacheLoadStrategy>(strategy);
    assert(cacheStrategy);
    return cacheStrategy->GetBlockCacheOption().memorySize;
}

void LoadConfigList::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("load_config", _impl->loadConfigs, _impl->loadConfigs);
    if (json.GetMode() == FROM_JSON) {
        _impl->totalCacheMemorySize = 0;
        for (size_t i = 0; i < _impl->loadConfigs.size(); ++i) {
            LoadConfig& loadConfig = _impl->loadConfigs[i];
            loadConfig.GetLoadStrategy()->SetEnableLoadSpeedLimit(_impl->enableLoadSpeedLimit);
            if (loadConfig.GetName().empty()) {
                loadConfig.SetName(std::string("load_config") + autil::StringUtil::toString(i));
            }
            _impl->totalCacheMemorySize += GetMemorySize(loadConfig);
        }
    }
}

void LoadConfigList::Check() const
{
    std::set<std::string> nameSet;
    for (size_t i = 0; i < _impl->loadConfigs.size(); ++i) {
        _impl->loadConfigs[i].Check();
        const std::string& name = _impl->loadConfigs[i].GetName();
        if (nameSet.count(name) > 0) {
            INDEXLIB_FATAL_ERROR(BadParameter, "duplicate load config name [%s]", name.c_str());
        }
        nameSet.insert(name);
    }
}

void LoadConfigList::PushFront(const LoadConfig& loadConfig)
{
    loadConfig.GetLoadStrategy()->SetEnableLoadSpeedLimit(_impl->enableLoadSpeedLimit);
    _impl->loadConfigs.insert(_impl->loadConfigs.begin(), loadConfig);
    _impl->totalCacheMemorySize += GetMemorySize(loadConfig);
}

void LoadConfigList::PushBack(const LoadConfig& loadConfig)
{
    loadConfig.GetLoadStrategy()->SetEnableLoadSpeedLimit(_impl->enableLoadSpeedLimit);
    _impl->loadConfigs.push_back(loadConfig);
    _impl->totalCacheMemorySize += GetMemorySize(loadConfig);
}

void LoadConfigList::SetLoadMode(LoadConfig::LoadMode mode)
{
    for (size_t i = 0; i < _impl->loadConfigs.size(); ++i) {
        _impl->loadConfigs[i].SetLoadMode(mode);
    }
    _impl->defaultLoadConfig.SetLoadMode(mode);
}

bool LoadConfigList::NeedLoadWithLifeCycle() const
{
    for (size_t i = 0; i < _impl->loadConfigs.size(); ++i) {
        if (!_impl->loadConfigs[i].GetLifecycle().empty()) {
            return true;
        }
    }
    if (!_impl->defaultLoadConfig.GetLifecycle().empty()) {
        return true;
    }
    return false;
}

void LoadConfigList::SetDefaultRootPath(const std::string& defaultLocalRoot, const std::string& defaultRemoteRoot)
{
    for (size_t i = 0; i < _impl->loadConfigs.size(); ++i) {
        LoadConfig& loadConfig = _impl->loadConfigs[i];
        if (loadConfig.GetLocalRootPath().empty()) {
            loadConfig.SetLocalRootPath(defaultLocalRoot);
        }
        if (loadConfig.GetRemoteRootPath().empty()) {
            loadConfig.SetRemoteRootPath(defaultRemoteRoot);
        }
    }
    if (_impl->defaultLoadConfig.GetLocalRootPath().empty()) {
        _impl->defaultLoadConfig.SetLocalRootPath(defaultLocalRoot);
    }
    if (_impl->defaultLoadConfig.GetRemoteRootPath().empty()) {
        _impl->defaultLoadConfig.SetRemoteRootPath(defaultRemoteRoot);
    }
}

const LoadConfigList::LoadConfigVector& LoadConfigList::GetLoadConfigs() const { return _impl->loadConfigs; }
const LoadConfig& LoadConfigList::GetLoadConfig(size_t idx) const
{
    assert(idx < _impl->loadConfigs.size());
    return _impl->loadConfigs[idx];
}
const LoadConfig& LoadConfigList::GetDefaultLoadConfig() const { return _impl->defaultLoadConfig; }
size_t LoadConfigList::GetTotalCacheMemorySize() const { return _impl->totalCacheMemorySize; }
void LoadConfigList::Clear() { _impl->loadConfigs.clear(); }
size_t LoadConfigList::Size() const { return _impl->loadConfigs.size(); }
void LoadConfigList::SwitchLoadSpeedLimit(bool on) { *(_impl->enableLoadSpeedLimit) = on; }

LoadConfigList::LoadConfigVector& LoadConfigList::GetLoadConfigs() { return _impl->loadConfigs; }

LoadConfig LoadConfigList::MakeMmapLoadConfig(const std::vector<std::string>& patterns, bool warmup, bool isLock,
                                              bool adviseRandom, uint32_t lockSlice, uint32_t lockInterval)
{
    LoadConfig loadConfig;
    loadConfig.SetLoadStrategyPtr(std::make_shared<MmapLoadStrategy>(isLock, adviseRandom, lockSlice, lockInterval));
    loadConfig.SetFilePatternString(patterns);
    WarmupStrategy warmupStrategy;
    warmupStrategy.SetWarmupType(warmup ? WarmupStrategy::WARMUP_SEQUENTIAL : WarmupStrategy::WARMUP_NONE);
    loadConfig.SetWarmupStrategy(warmupStrategy);
    return loadConfig;
}

}} // namespace indexlib::file_system
