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

#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/file_system/load_config/WarmupStrategy.h"
#include "indexlib/util/RegularExpression.h"

namespace indexlib { namespace file_system {

class LoadConfig : public autil::legacy::Jsonizable
{
public:
    enum class LoadMode {
        REMOTE_ONLY,
        LOCAL_ONLY,
    };
    typedef std::vector<std::string> FilePatternStringVector;

public:
    LoadConfig();
    LoadConfig(const LoadConfig& other);
    LoadConfig& operator=(const LoadConfig& other);
    ~LoadConfig();

public:
    void Check() const;
    bool EqualWith(const LoadConfig& other) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool Match(const std::string& filePath, const std::string& lifecycle) const;

public:
    bool IsRemote() const;
    bool NeedDeploy() const;
    const LoadStrategyPtr& GetLoadStrategy() const;
    const WarmupStrategy& GetWarmupStrategy() const;
    const FilePatternStringVector& GetFilePatternString() const;
    const util::RegularExpressionVector& GetFilePatternRegex() const;
    const std::string& GetName() const;
    const std::string& GetLoadStrategyName() const;
    const std::string& GetLocalRootPath() const;
    const std::string& GetRemoteRootPath() const;
    const std::string& GetLifecycle() const;

public:
    void SetRemote(bool remote);
    void SetDeploy(bool deploy);
    void SetLoadStrategyPtr(const LoadStrategyPtr& loadStrategy);
    void SetWarmupStrategy(const WarmupStrategy& warmupStrategy);
    void SetFilePatternString(const FilePatternStringVector& filePatterns);
    void SetLoadMode(LoadMode mode);
    void SetName(const std::string name);
    void SetLocalRootPath(const std::string& rootPath);
    void SetRemoteRootPath(const std::string& rootPath);
    void SetLifecycle(const std::string lifecycle);

public:
    static const std::string& BuiltInPatternToRegexPattern(const std::string& builtInPattern);

private:
    static void CreateRegularExpressionVector(const FilePatternStringVector& patternVector,
                                              util::RegularExpressionVector& regexVector);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
    friend class LoadConfigTest;
};
typedef std::shared_ptr<LoadConfig> LoadConfigPtr;

}} // namespace indexlib::file_system
