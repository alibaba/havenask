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
#include "indexlib/config/BuildConfig.h"

#include <cstdint>
#include <limits>

#include "autil/EnvUtil.h"
#include "autil/legacy/exception.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, BuildConfig);

struct BuildConfig::Impl {
    int64_t buildingMemoryLimitMB = -1;
    // fence timestamp deviation tolerance for clock in different machine.
    int64_t fenceTsTolerantDeviation = 3600l * 1000 * 1000;
    uint64_t maxDocCount = std::numeric_limits<uint64_t>::max();
    uint32_t keepVersionCount = 2;
    uint32_t keepVersionHour = 0;
    // keep versions determined by keepVersionCount, keepVersionHour and keepDailyCheckpointCount
    int32_t maxCommitRetryCount = 5;
    bool enablePackageFile = true; // DEFAULT: true for offline, false for online
    uint32_t dumpThreadCount = 4;  // used for offline build
    bool tolerateDocError = true;
};

void BuildConfig::InitForOnline() { _impl->enablePackageFile = false; }

void BuildConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("keep_version_count", _impl->keepVersionCount, _impl->keepVersionCount);
    json.Jsonize("keep_version_hour", _impl->keepVersionHour, _impl->keepVersionHour);
    json.Jsonize("fence_timestamp_tolerant_deviation", _impl->fenceTsTolerantDeviation,
                 _impl->fenceTsTolerantDeviation);
    json.Jsonize("enable_package_file", _impl->enablePackageFile, _impl->enablePackageFile);
    json.Jsonize("max_doc_count", _impl->maxDocCount, _impl->maxDocCount);
    json.Jsonize("building_memory_limit_mb", _impl->buildingMemoryLimitMB, _impl->buildingMemoryLimitMB);
    json.Jsonize("dump_thread_count", _impl->dumpThreadCount, _impl->dumpThreadCount);
    json.Jsonize("tolerate_doc_error", _impl->tolerateDocError, _impl->tolerateDocError);
    Check();
}

void BuildConfig::Check() const
{
    if (_impl->maxDocCount == 0) {
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "max_doc_count should not be zero");
    }
    if (_impl->keepVersionCount == 0) {
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "keep_version_count should be greater than 0.");
    }
}

BuildConfig::BuildConfig() : _impl(std::make_unique<BuildConfig::Impl>()) { TEST_init(); }
BuildConfig::BuildConfig(const BuildConfig& other) : _impl(std::make_unique<BuildConfig::Impl>(*other._impl)) {}
BuildConfig& BuildConfig::operator=(const BuildConfig& other)
{
    if (this != &other) {
        _impl = std::make_unique<BuildConfig::Impl>(*other._impl);
    }
    return *this;
}
BuildConfig::~BuildConfig() {}

void BuildConfig::TEST_init()
{
    if (autil::EnvUtil::getEnv("IS_INDEXLIB_TEST_MODE", false)) {
        _impl->buildingMemoryLimitMB = 64;
    }
}

uint64_t BuildConfig::GetMaxDocCount() const { return _impl->maxDocCount; }
int64_t BuildConfig::GetBuildingMemoryLimit() const
{
    if (_impl->buildingMemoryLimitMB != -1) {
        return _impl->buildingMemoryLimitMB * 1024 * 1024;
    }
    return _impl->buildingMemoryLimitMB;
}
uint32_t BuildConfig::GetKeepVersionCount() const { return _impl->keepVersionCount; }
uint32_t BuildConfig::GetKeepVersionHour() const { return _impl->keepVersionHour; }
int64_t BuildConfig::GetFenceTsTolerantDeviation() const { return _impl->fenceTsTolerantDeviation; }
int32_t BuildConfig::GetMaxCommitRetryCount() const { return _impl->maxCommitRetryCount; }
bool BuildConfig::GetIsEnablePackageFile() const { return _impl->enablePackageFile; }
bool BuildConfig::GetIsTolerateDocError() const { return _impl->tolerateDocError; }
uint32_t BuildConfig::GetDumpThreadCount() const { return _impl->dumpThreadCount; }

void BuildConfig::TEST_SetBuildingMemoryLimit(int64_t limit) { _impl->buildingMemoryLimitMB = limit / 1024 / 1024; }
void BuildConfig::TEST_SetMaxDocCount(uint64_t count) { _impl->maxDocCount = count; }
void BuildConfig::TEST_SetKeepVersionCount(uint32_t count) { _impl->keepVersionCount = count; }
void BuildConfig::TEST_SetFenceTsTolerantDeviation(int64_t tolerantDeviation)
{
    _impl->fenceTsTolerantDeviation = tolerantDeviation;
}
void BuildConfig::TEST_SetEnablePackageFile(bool enable) { _impl->enablePackageFile = enable; }
void BuildConfig::TEST_SetDumpThreadCount(uint32_t count) { _impl->dumpThreadCount = count; }

} // namespace indexlibv2::config
