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
#include <vector>

#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/DeployIndexMeta.h"

namespace indexlibv2::framework {

class DeployIndexUtil
{
public:
    static std::pair<Status, int64_t> GetIndexSize(const std::string& rawPath, const versionid_t targetVersion);

    // new class IndexSubscriber?
    static bool NeedSubscribeRemoteIndex(const std::string& remoteIndexRoot);
    static void SubscribeRemoteIndex(const std::string& remoteIndexRoot, versionid_t remoteVersionId);

private:
    static Status GetDeployIndexMeta(const std::string& rawPath, versionid_t versionId,
                                     indexlib::file_system::DeployIndexMetaVec* localDeployIndexMetaVec,
                                     indexlib::file_system::DeployIndexMetaVec* remoteDeployIndexMetaVec);
};

} // namespace indexlibv2::framework
