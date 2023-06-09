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
#include <mutex>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/framework/VersionDeployDescription.h"

namespace indexlibv2 {
namespace config {
class TabletOptions;
}

namespace framework {
class TabletDeployer : private autil::NoCopyable
{
public:
    struct GetDeployIndexMetaInputParams {
        std::string rawPath;    // raw index partition path, index produced here
        std::string localPath;  // local partition path
        std::string remotePath; // index partition path came from suze target, refer to mpangu, dcache or equal rawPath,
                                // just default value
        const config::TabletOptions* baseTabletOptions = nullptr;
        const config::TabletOptions* targetTabletOptions = nullptr;
        versionid_t baseVersionId = INVALID_VERSIONID;
        versionid_t targetVersionId = INVALID_VERSIONID;
        indexlibv2::framework::VersionDeployDescription baseVersionDeployDescription;
    };

    struct GetDeployIndexMetaOutputParams {
        indexlib::file_system::DeployIndexMetaVec localDeployIndexMetaVec;
        indexlib::file_system::DeployIndexMetaVec remoteDeployIndexMetaVec;
        indexlibv2::framework::VersionDeployDescription versionDeployDescription;
    };

    static bool GetDeployIndexMeta(const GetDeployIndexMetaInputParams& inputParams,
                                   GetDeployIndexMetaOutputParams* outputParams) noexcept;
};
} // namespace framework
} // namespace indexlibv2
