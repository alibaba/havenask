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
#include "indexlib/config/offline_config_base.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, OfflineConfigBase);

OfflineConfigBase::OfflineConfigBase()
    : enableRecoverIndex(true)
    , recoverType(RecoverStrategyType::RST_SEGMENT_LEVEL)
    , fullIndexStoreKKVTs(false)
{
}

OfflineConfigBase::OfflineConfigBase(const BuildConfig& buildConf, const MergeConfig& mergeConf)
    : buildConfig(buildConf)
    , mergeConfig(mergeConf)
    , enableRecoverIndex(true)
    , recoverType(RecoverStrategyType::RST_SEGMENT_LEVEL)
    , fullIndexStoreKKVTs(false)
{
}

OfflineConfigBase::~OfflineConfigBase() {}

void OfflineConfigBase::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize("build_config", buildConfig, buildConfig);
    json.Jsonize("merge_config", mergeConfig, mergeConfig);
    json.Jsonize("full_index_store_kkv_ts", fullIndexStoreKKVTs, fullIndexStoreKKVTs);
    json.Jsonize("offline_reader_config", readerConfig, readerConfig);
}

void OfflineConfigBase::Check() const
{
    buildConfig.Check();
    mergeConfig.Check();
}
}} // namespace indexlib::config
