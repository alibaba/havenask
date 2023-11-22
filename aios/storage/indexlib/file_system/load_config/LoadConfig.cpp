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
#include "indexlib/file_system/load_config/LoadConfig.h"

#include <map>

#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LoadConfig);

struct LoadConfig::Impl {
    bool remote = false; // read from remote
    bool deploy = true;  // need deploy to local
    LoadStrategyPtr loadStrategy = std::make_shared<MmapLoadStrategy>();
    WarmupStrategy warmupStrategy;
    LoadConfig::FilePatternStringVector filePatterns;
    // abstract warmup strategy to a class when it's geting complex
    util::RegularExpressionVector regexVector;
    std::string name;
    std::string lifecycle;
    std::string localRootPath;  // local partition path
    std::string remoteRootPath; // remote partition path
};

LoadConfig::LoadConfig() : _impl(std::make_unique<LoadConfig::Impl>()) {}
LoadConfig::LoadConfig(const LoadConfig& other) : _impl(std::make_unique<LoadConfig::Impl>(*other._impl)) {}
LoadConfig& LoadConfig::operator=(const LoadConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<LoadConfig::Impl>(*other._impl);
    }
    return *this;
}
LoadConfig::~LoadConfig() {}

const std::string& LoadConfig::BuiltInPatternToRegexPattern(const std::string& builtInPattern)
{
    // TODO: support register by index
    static const std::unordered_map<std::string, std::string> builtInPatternMap = {
        {"_PATCH_", SEGMENT_SUB_PATTERN + "/(attribute|index)/\\w+/(\\w+/)?[0-9]+_[0-9]+\\.patch"},
        {"_ATTRIBUTE_", SEGMENT_SUB_PATTERN + "/attribute/"},
        {"_ATTRIBUTE_DATA_", SEGMENT_SUB_PATTERN + "/attribute/\\w+/(slice_[0-9]+/)?data"},
        {"_ATTRIBUTE_OFFSET_", SEGMENT_SUB_PATTERN + "/attribute/\\w+/(slice_[0-9]+/)?offset"},
        {"_PK_ATTRIBUTE_", SEGMENT_SUB_PATTERN + "/index/\\w+/attribute_"},
        {"_SECTION_ATTRIBUTE_", SEGMENT_SUB_PATTERN + "/index/\\w+_section/"},
        {"_INDEX_", SEGMENT_SUB_PATTERN + "/index/"},
        {"_INDEX_DICTIONARY_", SEGMENT_SUB_PATTERN + "/index/\\w+/dictionary"},
        {"_INDEX_POSTING_", SEGMENT_SUB_PATTERN + "/index/\\w+/posting"},
        {"_SUMMARY_", SEGMENT_SUB_PATTERN + "/summary/"},
        {"_SUMMARY_DATA_", SEGMENT_SUB_PATTERN + "/summary/data$"},
        {"_SUMMARY_OFFSET_", SEGMENT_SUB_PATTERN + "/summary/offset$"},
        {"_SOURCE_", SEGMENT_SUB_PATTERN + "/source/"},
        {"_SOURCE_DATA_", SEGMENT_SUB_PATTERN + "/source/group_[0-9]+/data$"},
        {"_SOURCE_META_", SEGMENT_SUB_PATTERN + "/source/meta/data$"},
        {"_SOURCE_OFFSET_", SEGMENT_SUB_PATTERN + "/source/group_[0-9]+/offset$"},
        {"_KV_KEY_", SEGMENT_KV_PATTERN + "/index/\\w+/key"},
        {"_KV_VALUE_", SEGMENT_KV_PATTERN + "/index/\\w+/value"},
        {"_KV_VALUE_COMPRESS_META_", SEGMENT_KV_PATTERN + "/index/\\w+/value" + COMPRESS_FILE_META_SUFFIX},
        {"_KKV_PKEY_", SEGMENT_KV_PATTERN + "/index/\\w+/pkey"},
        {"_KKV_SKEY_", SEGMENT_KV_PATTERN + "/index/\\w+/skey"},
        {"_KKV_VALUE_", SEGMENT_KV_PATTERN + "/index/\\w+/value"},
    };

    const auto& it = builtInPatternMap.find(builtInPattern);
    if (it != builtInPatternMap.end()) {
        return it->second;
    }
    return builtInPattern;
}

void LoadConfig::CreateRegularExpressionVector(const LoadConfig::FilePatternStringVector& patternVector,
                                               util::RegularExpressionVector& regexVector)
{
    regexVector.clear();

    for (size_t i = 0; i < patternVector.size(); ++i) {
        util::RegularExpressionPtr regex(new util::RegularExpression);
        std::string pattern = BuiltInPatternToRegexPattern(patternVector[i]);
        if (!regex->Init(pattern)) {
            INDEXLIB_THROW(util::BadParameterException,
                           "Invalid parameter for regular expression pattern[%s], error[%s]", patternVector[i].c_str(),
                           regex->GetLatestErrMessage().c_str());
        }
        regexVector.push_back(regex);
    }
}

void LoadConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("file_patterns", _impl->filePatterns);
    json.Jsonize("name", _impl->name, _impl->name);
    json.Jsonize("remote", _impl->remote, _impl->remote);
    json.Jsonize("deploy", _impl->deploy, _impl->deploy);
    json.Jsonize("lifecycle", _impl->lifecycle, _impl->lifecycle);
    // need table/generation/partition suffix
    json.Jsonize("remote_root", _impl->remoteRootPath, _impl->remoteRootPath);

    if (json.GetMode() == FROM_JSON) {
        std::string warmupStr;
        json.Jsonize("warmup_strategy", warmupStr, WarmupStrategy::ToTypeString(WarmupStrategy::WARMUP_NONE));
        _impl->warmupStrategy.SetWarmupType(WarmupStrategy::FromTypeString(warmupStr));
    } else {
        std::string warmupStr = WarmupStrategy::ToTypeString(_impl->warmupStrategy.GetWarmupType());
        json.Jsonize("warmup_strategy", warmupStr);
    }
    if (json.GetMode() == FROM_JSON) {
        CreateRegularExpressionVector(_impl->filePatterns, _impl->regexVector);
    }

    std::string loadStrategyName = GetLoadStrategyName();
    if (loadStrategyName == READ_MODE_GLOBAL_CACHE) {
        loadStrategyName = READ_MODE_CACHE;
    }
    json.Jsonize("load_strategy", loadStrategyName, READ_MODE_MMAP);
    if (json.GetMode() == FROM_JSON) {
        if (loadStrategyName == READ_MODE_MMAP) {
            auto loadStrategy = std::make_shared<MmapLoadStrategy>();
            json.Jsonize("load_strategy_param", *loadStrategy, *loadStrategy);
            _impl->loadStrategy = loadStrategy;
        } else if (loadStrategyName == READ_MODE_CACHE) {
            CacheLoadStrategyPtr loadStrategy(new CacheLoadStrategy());
            json.Jsonize("load_strategy_param", *loadStrategy, *loadStrategy);
            _impl->loadStrategy = loadStrategy;
        } else {
            INDEXLIB_THROW(util::BadParameterException, "Invalid parameter for load_strategy[%s]",
                           loadStrategyName.c_str());
        }
    } else {
        // TO_JSON
        if (_impl->loadStrategy) {
            json.Jsonize("load_strategy_param", *_impl->loadStrategy);
        }
    }
}

bool LoadConfig::EqualWith(const LoadConfig& other) const
{
    assert(_impl.get());
    assert(other._impl.get());
    assert(other._impl->loadStrategy);
    assert(_impl->loadStrategy);

    if (!(_impl->warmupStrategy == other._impl->warmupStrategy)) {
        return false;
    }
    if (!_impl->loadStrategy->EqualWith(other._impl->loadStrategy)) {
        return false;
    }
    if (_impl->filePatterns != other._impl->filePatterns) {
        return false;
    }
    return _impl->lifecycle == other._impl->lifecycle;
}

void LoadConfig::Check() const
{
    assert(_impl.get());
    if (!_impl->deploy && !_impl->remote && _impl->name != DISABLE_FIELDS_LOAD_CONFIG_NAME) {
        INDEXLIB_FATAL_ERROR(BadParameter, "loadConfig [%s] [deploy] and [remote] can not be false simultaneously",
                             _impl->name.c_str());
    }
    _impl->loadStrategy->Check();
}

bool LoadConfig::Match(const std::string& filePath, const std::string& lifecycle) const
{
    for (size_t i = 0, size = _impl->regexVector.size(); i < size; ++i) {
        if (_impl->regexVector[i]->Match(filePath)) {
            return _impl->lifecycle.empty() || lifecycle == _impl->lifecycle;
        }
    }
    return false;
}

bool LoadConfig::IsRemote() const { return _impl->remote; }
bool LoadConfig::NeedDeploy() const { return _impl->deploy; }
const LoadStrategyPtr& LoadConfig::GetLoadStrategy() const { return _impl->loadStrategy; }
const WarmupStrategy& LoadConfig::GetWarmupStrategy() const { return _impl->warmupStrategy; }
const LoadConfig::FilePatternStringVector& LoadConfig::GetFilePatternString() const { return _impl->filePatterns; }
const util::RegularExpressionVector& LoadConfig::GetFilePatternRegex() const { return _impl->regexVector; }
const std::string& LoadConfig::GetName() const { return _impl->name; }
const std::string& LoadConfig::GetLoadStrategyName() const { return _impl->loadStrategy->GetLoadStrategyName(); }
const std::string& LoadConfig::GetLocalRootPath() const { return _impl->localRootPath; }
const std::string& LoadConfig::GetRemoteRootPath() const { return _impl->remoteRootPath; }
const std::string& LoadConfig::GetLifecycle() const { return _impl->lifecycle; }

void LoadConfig::SetRemote(bool remote) { _impl->remote = remote; }
void LoadConfig::SetDeploy(bool deploy) { _impl->deploy = deploy; }
void LoadConfig::SetLoadStrategyPtr(const LoadStrategyPtr& loadStrategy) { _impl->loadStrategy = loadStrategy; }
void LoadConfig::SetWarmupStrategy(const WarmupStrategy& warmupStrategy) { _impl->warmupStrategy = warmupStrategy; }
void LoadConfig::SetFilePatternString(const LoadConfig::FilePatternStringVector& filePatterns)
{
    _impl->filePatterns = filePatterns;
    CreateRegularExpressionVector(_impl->filePatterns, _impl->regexVector);
}
void LoadConfig::SetLoadMode(LoadMode mode)
{
    assert(mode == LoadMode::LOCAL_ONLY || mode == LoadMode::REMOTE_ONLY);
    if (mode == LoadMode::LOCAL_ONLY) {
        _impl->remote = false;
        _impl->deploy = true;
    } else if (mode == LoadMode::REMOTE_ONLY) {
        _impl->remote = true;
        _impl->deploy = false;
    }
}
void LoadConfig::SetName(const std::string name) { _impl->name = name; }
void LoadConfig::SetLocalRootPath(const std::string& rootPath) { _impl->localRootPath = rootPath; }
void LoadConfig::SetRemoteRootPath(const std::string& rootPath) { _impl->remoteRootPath = rootPath; }
void LoadConfig::SetLifecycle(const std::string lifecycle) { _impl->lifecycle = lifecycle; }

}} // namespace indexlib::file_system
