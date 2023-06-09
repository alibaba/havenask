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
#ifndef __INDEXLIB_OFFLINE_CONFIG_H
#define __INDEXLIB_OFFLINE_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/module_info.h"
#include "indexlib/config/offline_config_base.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, OfflineConfigImpl);
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
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_OFFLINE_CONFIG_H
