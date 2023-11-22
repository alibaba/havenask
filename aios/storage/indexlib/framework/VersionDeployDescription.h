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
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/LifecycleTable.h"

namespace indexlib::file_system {
class DeployIndexMeta;
} // namespace indexlib::file_system

namespace indexlibv2::framework {

struct VersionDeployDescription : public autil::legacy::Jsonizable {
    VersionDeployDescription()
        : editionId(CURRENT_EDITION_ID)
        , deployTime(-1)
        , lifecycleTable(std::make_shared<indexlib::file_system::LifecycleTable>())
    {
    }
    ~VersionDeployDescription() {}

public:
    enum struct FeatureType : uint32_t {
        DEPLOY_META_MANIFEST = 0,
        DEPLOY_TIME = 1,
    };

public:
    static bool IsJson(const std::string& fileContent);
    static uint32_t CURRENT_EDITION_ID;
    static std::string DONE_FILE_NAME_PREFIX;
    static std::string DONE_FILE_NAME_SUFFIX;

public:
    bool SupportFeature(FeatureType id) const;
    bool DisableFeature(FeatureType id);
    bool Deserialize(const std::string& content);
    void Jsonize(JsonWrapper& json) override;
    bool CheckDeployDone(const std::string& fileContent) const;
    bool CheckDeployMetaEquality(const VersionDeployDescription& other) const;
    bool CheckLifecycleTableEquality(const VersionDeployDescription& other) const;
    bool CheckDeployDoneByLegacyContent(const std::string& legacyContent) const;
    bool CheckDeployDoneByEdition(uint32_t editionId, const VersionDeployDescription& other) const;
    void GetLocalDeployFileList(std::set<std::string>* localDeployFiles);
    const std::shared_ptr<indexlib::file_system::LifecycleTable>& GetLifecycleTable() const { return lifecycleTable; }
    static bool LoadDeployDescription(const std::string& rootPath, versionid_t versionId,
                                      VersionDeployDescription* versionDpDesc);

    static std::string GetDoneFileNameByVersionId(versionid_t versionId);
    static bool ExtractVersionId(const std::string& fileName, versionid_t& versionId);

    uint32_t editionId;
    int64_t deployTime;
    std::string rawPath;
    std::string remotePath;
    std::string configPath;
    std::vector<std::shared_ptr<indexlib::file_system::DeployIndexMeta>> localDeployIndexMetas;
    std::vector<std::shared_ptr<indexlib::file_system::DeployIndexMeta>> remoteDeployIndexMetas;
    std::shared_ptr<indexlib::file_system::LifecycleTable> lifecycleTable;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<VersionDeployDescription> VersionDeployDescriptionPtr;

} // namespace indexlibv2::framework
