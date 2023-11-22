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
#include "indexlib/config/merge_config.h"

#include "indexlib/config/impl/merge_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, MergeConfig);

MergeConfig::MergeConfig() { mImpl.reset(new MergeConfigImpl); }

MergeConfig::MergeConfig(const MergeConfig& other) : MergeConfigBase(other)
{
    mImpl.reset(new MergeConfigImpl(*other.mImpl));
}

MergeConfig::~MergeConfig() {}

void MergeConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    MergeConfigBase::Jsonize(json);
    mImpl->Jsonize(json);
}

void MergeConfig::Check() const
{
    MergeConfigBase::Check();
    mImpl->Check();
}

bool MergeConfig::IsArchiveFileEnable() const { return mImpl->IsArchiveFileEnable(); }

void MergeConfig::SetEnableArchiveFile(bool value) const { return mImpl->SetEnableArchiveFile(value); }

void MergeConfig::operator=(const MergeConfig& other)
{
    *(MergeConfigBase*)this = (const MergeConfigBase&)other;
    *mImpl = *(other.mImpl);
}

ModuleClassConfig& MergeConfig::GetSplitSegmentConfig() { return mImpl->splitSegmentConfig; }
std::string MergeConfig::GetSplitStrategyName() const { return mImpl->splitSegmentConfig.className; };
bool MergeConfig::GetEnablePackageFile() const { return mImpl->enablePackageFile; }
const file_system::PackageFileTagConfigList& MergeConfig::GetPackageFileTagConfigList() const
{
    return mImpl->packageFileTagConfigList;
}
uint32_t MergeConfig::GetCheckpointInterval() const { return mImpl->checkpointInterval; }

void MergeConfig::SetEnablePackageFile(bool value) { mImpl->enablePackageFile = value; }
void MergeConfig::SetCheckpointInterval(uint32_t value) { mImpl->checkpointInterval = value; }
void MergeConfig::SetEnableInPlanMultiSegmentParallel(bool value) { mImpl->enableInPlanMultiSegmentParallel = value; }
bool MergeConfig::EnableInPlanMultiSegmentParallel() const { return mImpl->enableInPlanMultiSegmentParallel; }

bool MergeConfig::NeedCalculateTemperature() const { return mImpl->needCalculateTemperature; }
void MergeConfig::SetCalculateTemperature(const bool& isCalculate) { mImpl->needCalculateTemperature = isCalculate; }
}} // namespace indexlib::config
