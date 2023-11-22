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

#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/LoadConfig.h"

namespace indexlib { namespace file_system {

class LoadConfigList : public autil::legacy::Jsonizable
{
public:
    LoadConfigList();
    LoadConfigList(const LoadConfigList& other);
    LoadConfigList& operator=(const LoadConfigList& other);
    ~LoadConfigList();

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    typedef std::vector<LoadConfig> LoadConfigVector;

    const LoadConfigVector& GetLoadConfigs() const;
    const LoadConfig& GetDefaultLoadConfig() const;

    size_t Size() const;
    const LoadConfig& GetLoadConfig(size_t idx) const;
    LoadConfigVector& GetLoadConfigs();

    void PushFront(const LoadConfig& loadConfig);
    void PushBack(const LoadConfig& loadConfig);
    void Clear();

    bool NeedLoadWithLifeCycle() const;
    size_t GetTotalCacheMemorySize() const;

    void SetLoadMode(LoadConfig::LoadMode mode);
    // root without fence, after PathUtil::NormalizePath, eg. .../table/generation/partition
    void SetDefaultRootPath(const std::string& defaultLocalRoot, const std::string& defaultRemoteRoot);
    void SwitchLoadSpeedLimit(bool on);

public:
    // ADVICE: adviseRandom=false, lockSlice=4*1024*1024, lockInterval=0
    static LoadConfig MakeMmapLoadConfig(const std::vector<std::string>& patterns, bool warmup, bool isLock,
                                         bool adviseRandom, uint32_t lockSlice, uint32_t lockInterval);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<LoadConfigList> LoadConfigListPtr;

}} // namespace indexlib::file_system
