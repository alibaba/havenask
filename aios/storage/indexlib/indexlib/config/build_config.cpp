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
#include "indexlib/config/build_config.h"

#include "autil/StringUtil.h"
#include "indexlib/config/impl/build_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, BuildConfig);

BuildConfig::BuildConfig() { mImpl.reset(new BuildConfigImpl); }

BuildConfig::BuildConfig(const BuildConfig& other) : BuildConfigBase(other)
{
    mImpl.reset(new BuildConfigImpl(*other.mImpl));
}

BuildConfig::~BuildConfig() {}

void BuildConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    BuildConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
}

void BuildConfig::Check() const
{
    BuildConfigBase::Check();
    mImpl->Check();
}

vector<ModuleClassConfig>& BuildConfig::GetSegmentMetricsUpdaterConfig() { return mImpl->segAttrUpdaterConfig; }

const std::map<std::string, std::string>& BuildConfig::GetCustomizedParams() const
{
    return mImpl->GetCustomizedParams();
}

bool BuildConfig::SetCustomizedParams(const std::string& key, const std::string& value)
{
    return mImpl->SetCustomizedParams(key, value);
}

bool BuildConfig::GetBloomFilterParamForPkReader(uint32_t& multipleNum) const
{
    return mImpl->GetBloomFilterParamForPkReader(multipleNum);
}

void BuildConfig::EnableBloomFilterForPkReader(uint32_t multipleNum)
{
    mImpl->EnableBloomFilterForPkReader(multipleNum);
}

void BuildConfig::SetAsyncDump(bool enable) { mImpl->SetAsyncDump(enable); }

bool BuildConfig::EnableAsyncDump() const { return mImpl->EnableAsyncDump(); }

void BuildConfig::EnablePkBuildParallelLookup() { return mImpl->EnablePkBuildParallelLookup(); }
bool BuildConfig::IsPkBuildParallelLookup() const { return mImpl->IsPkBuildParallelLookup(); }

bool BuildConfig::operator==(const BuildConfig& other) const
{
    return (*mImpl == *other.mImpl) && ((BuildConfigBase*)this)->operator==((const BuildConfigBase&)other);
}

void BuildConfig::operator=(const BuildConfig& other)
{
    *(BuildConfigBase*)this = (const BuildConfigBase&)other;
    *mImpl = *(other.mImpl);
}

int64_t BuildConfig::GetBuildingMemoryLimit() const { return mImpl->buildingMemoryLimit; }
void BuildConfig::SetBuildingMemoryLimit(int64_t memLimit) { mImpl->buildingMemoryLimit = memLimit; }
bool BuildConfig::IsBatchBuildEnabled(bool isConsistentMode) const
{
    return isConsistentMode ? mImpl->enableConsistentModeBatchBuild : mImpl->enableInconsistentModeBatchBuild;
}

void BuildConfig::SetBatchBuild(bool isConsistentMode, bool enableBatchBuild)
{
    if (isConsistentMode) {
        mImpl->enableConsistentModeBatchBuild = enableBatchBuild;
    }
    mImpl->enableInconsistentModeBatchBuild = enableBatchBuild;
}

int32_t BuildConfig::GetBatchBuildMaxThreadCount(bool isConsistentMode) const
{
    return isConsistentMode ? mImpl->batchBuildConsistentModeMaxThreadCount
                            : mImpl->batchBuildInconsistentModeMaxThreadCount;
}
void BuildConfig::SetBatchBuildMaxThreadCount(bool isConsistentMode, int32_t maxThreadCount)
{
    if (isConsistentMode) {
        mImpl->batchBuildConsistentModeMaxThreadCount = maxThreadCount;
    } else {
        mImpl->batchBuildInconsistentModeMaxThreadCount = maxThreadCount;
    }
}
int32_t BuildConfig::GetBatchBuildMaxBatchSize() const { return mImpl->batchBuildMaxBatchSize; }
void BuildConfig::SetBatchBuildMaxBatchSize(int32_t maxBatchSize) { mImpl->batchBuildMaxBatchSize = maxBatchSize; }
int64_t BuildConfig::GetBatchBuildMaxCollectInterval() const { return mImpl->batchBuildMaxCollectInterval; }
void BuildConfig::SetBatchBuildMaxCollectInterval(int64_t maxCollectInterval)
{
    mImpl->batchBuildMaxCollectInterval = maxCollectInterval;
}
int64_t BuildConfig::GetBatchBuildMaxCollectMemoryInM() const { return mImpl->batchBuildMaxCollectMemoryMB; }
void BuildConfig::SetBatchBuildMaxCollectMemoryInM(int64_t memoryInM)
{
    mImpl->batchBuildMaxCollectMemoryMB = memoryInM;
}
}} // namespace indexlib::config
