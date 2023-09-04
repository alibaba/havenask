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
#include "indexlib/index_base/legacy_tablet_deployer_hook.h"

#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/hooks/TabletHooksCreator.h"
#include "indexlib/index_base/deploy_index_wrapper.h"

namespace indexlib { namespace index_base {

std::unique_ptr<framework::ITabletDeployerHook> LegacyTabletDeployerHook::Create() const
{
    return std::make_unique<LegacyTabletDeployerHook>();
}

void LegacyTabletDeployerHook::RewriteLoadConfigList(const std::string& rootPath,
                                                     const indexlibv2::config::TabletOptions& tabletOptions,
                                                     versionid_t versionId, const std::string& localPath,
                                                     const std::string& remotePath,
                                                     indexlib::file_system::LoadConfigList* loadConfigList)
{
    assert(loadConfigList);
    assert(tabletOptions.IsLegacy());
    // GenerateDisableLoadConfig for online_config.disable_fields
    file_system::LoadConfig loadConfigForDisable;
    if (DeployIndexWrapper::GenerateDisableLoadConfig(tabletOptions, loadConfigForDisable)) {
        loadConfigList->PushFront(loadConfigForDisable);
    }
    // GenerateDisableLoadConfig for dynamic del/add field
    file_system::LoadConfig loadConfigForDynamic;
    if (DeployIndexWrapper::GenerateDisableLoadConfig(rootPath, versionId, loadConfigForDynamic)) {
        loadConfigList->PushFront(loadConfigForDynamic);
    }
}

REGISTER_TABLET_DEPLOYER_HOOK(__LEGACY_TABLE__, LegacyTabletDeployerHook);

}} // namespace indexlib::index_base
