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
#include <stdint.h>
#include <string>
#include <unordered_map>

#include "autil/Log.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"

namespace indexlib { namespace file_system {
class EntryMeta;
class LifecycleTable;
class LoadConfigList;

class IndexFileDeployer
{
public:
    IndexFileDeployer(DeployIndexMetaVec* localDeployIndexMetaVec, DeployIndexMetaVec* remoteDeployIndexMetaVec);
    ~IndexFileDeployer();

public:
    ErrorCode FillDeployIndexMetaVec(versionid_t versionId, const std::string& physicalRoot,
                                     const LoadConfigList& loadConfigList,
                                     const std::shared_ptr<LifecycleTable>& lifecycleTable);

public:
    static FSResult<int32_t> GetLastVersionId(const std::string& physicalRoot);
    static FSResult<bool> IsDeployMetaExist(const std::string& physicalRoot, versionid_t versionId);

private:
    void FillByOneEntryMeta(const EntryMeta& entryMeta, const LoadConfigList& loadConfigList,
                            const std::shared_ptr<LifecycleTable>& lifecycleTable);

private:
    DeployIndexMetaVec* _localDeployIndexMetaVec;
    DeployIndexMetaVec* _remoteDeployIndexMetaVec;
    // physicalRoot -> DeployIndexMeta*
    // TODO: may be need {source, target} key
    std::unordered_map<std::string, DeployIndexMeta*> _remoteRootMap;
    std::unordered_map<std::string, DeployIndexMeta*> _localRootMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexFileDeployer> IndexFileDeployerPtr;
}} // namespace indexlib::file_system
