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

#include "autil/Log.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/offline_config_base.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib::config {
class OfflineConfigImpl;
typedef std::shared_ptr<OfflineConfigImpl> OfflineConfigImplPtr;
} // namespace indexlib::config
namespace indexlib { namespace config {

class OfflineConfig : public OfflineConfigBase
{
public:
    OfflineConfig();
    OfflineConfig(const OfflineConfig& other);
    OfflineConfig(const BuildConfig& buildConf, const MergeConfig& mergeConf);

    ~OfflineConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;
    void operator=(const OfflineConfig& other);

    void RebuildTruncateIndex();
    void RebuildAdaptiveBitmap();
    bool NeedRebuildTruncateIndex() const;
    bool NeedRebuildAdaptiveBitmap() const;
    const LoadConfigList& GetLoadConfigList() const;
    void SetLoadConfigList(const LoadConfigList& loadConfigList);

    const config::ModuleInfos& GetModuleInfos() const;

private:
    OfflineConfigImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<OfflineConfig> OfflineConfigPtr;
}} // namespace indexlib::config
