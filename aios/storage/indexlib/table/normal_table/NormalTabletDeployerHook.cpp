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
#include "indexlib/table/normal_table/NormalTabletDeployerHook.h"

#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/hooks/TabletHooksCreator.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/Constant.h"
#include "indexlib/index/summary/Common.h"

namespace indexlib::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletDeployerHook);

std::unique_ptr<framework::ITabletDeployerHook> NormalTabletDeployerHook::Create() const
{
    return std::make_unique<NormalTabletDeployerHook>();
}

class DisableFieldsConfig : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("attributes", attributes, attributes);
        json.Jsonize("indexes", indexes, indexes);
        json.Jsonize("pack_attributes", packAttributes, packAttributes);
        json.Jsonize("rewrite_load_config", rewriteLoadConfig, rewriteLoadConfig);

        std::string summaryDisableField;
        json.Jsonize("summarys", summaryDisableField, "__SUMMARY_FIELD_NONE__");
        summarys = (summaryDisableField == "__SUMMARY_FIELD_ALL__");

        std::string sourceDisableField;
        json.Jsonize("sources", sourceDisableField, "__SUMMARY_FIELD_NONE__");
        StrToDisableSource(sourceDisableField);
    }
    bool needDisable() const
    {
        return rewriteLoadConfig && (!attributes.empty() || !indexes.empty() || !packAttributes.empty() || summarys ||
                                     sources != CDF_FIELD_NONE);
    }
    std::unique_ptr<file_system::LoadConfig> CreateLoadConfig()
    {
        const std::string SEGMENT_P = "segment_[0-9]+(_level_[0-9]+)?(/sub_segment)?/";
        auto loadConfig = std::make_unique<file_system::LoadConfig>();
        loadConfig->SetLoadStrategyPtr(std::make_shared<file_system::CacheLoadStrategy>());
        loadConfig->SetRemote(false);
        loadConfig->SetDeploy(false);
        loadConfig->SetName("__DISABLE_FIELDS__");

        std::vector<std::string> filePatterns;
        for (const auto& attribute : attributes) {
            filePatterns.push_back(SEGMENT_P + index::ATTRIBUTE_INDEX_PATH + "/" + attribute + "($|/)");
        }
        for (const auto& packAttribute : packAttributes) {
            filePatterns.push_back(SEGMENT_P + index::ATTRIBUTE_INDEX_PATH + "/" + packAttribute + "($|/)");
        }
        for (const auto& index : indexes) {
            filePatterns.push_back(SEGMENT_P + index::INVERTED_INDEX_PATH + "/" + index + "($|/)");
        }
        if (summarys) {
            filePatterns.push_back(SEGMENT_P + index::SUMMARY_INDEX_PATH + "/");
        }
        if (sources == CDF_FIELD_ALL) {
            filePatterns.push_back(SEGMENT_P + index::SOURCE_INDEX_PATH + "/");
        }
        for (auto groupId : disabledSourceGroupIds) {
            auto groupDirName = std::string(index::SOURCE_DATA_DIR_PREFIX) + "_" + autil::StringUtil::toString(groupId);
            filePatterns.push_back(SEGMENT_P + index::SOURCE_INDEX_PATH + "/" + groupDirName + "($|/)");
        }
        loadConfig->SetFilePatternString(filePatterns);
        loadConfig->Check();
        return loadConfig;
    }

private:
    void StrToDisableSource(const std::string& str)
    {
        const std::string SOURCE_FIELD_GROUP = "__SOURCE_FIELD_GROUP__:";

        disabledSourceGroupIds.clear();
        if (str == "__SOURCE_FIELD_ALL__") {
            sources = CDF_FIELD_ALL;
        } else if (str.find(SOURCE_FIELD_GROUP) == 0) {
            std::string groupIdStr = str.substr(SOURCE_FIELD_GROUP.size());
            autil::StringUtil::fromString(groupIdStr, disabledSourceGroupIds, ",");
            sources = CDF_FIELD_GROUP;
        } else {
            sources = CDF_FIELD_NONE;
        }
    }

private:
    enum SourceDisableField { CDF_FIELD_NONE, CDF_FIELD_GROUP, CDF_FIELD_ALL };

    std::vector<std::string> attributes;
    std::vector<std::string> indexes;
    std::vector<std::string> packAttributes;
    bool rewriteLoadConfig = true;
    bool summarys = false;
    SourceDisableField sources = CDF_FIELD_NONE;
    std::vector<int32_t> disabledSourceGroupIds;
};

void NormalTabletDeployerHook::RewriteLoadConfigList(const std::string& rootPath,
                                                     const indexlibv2::config::TabletOptions& tabletOptions,
                                                     versionid_t versionId, const std::string& localPath,
                                                     const std::string& remotePath,
                                                     indexlib::file_system::LoadConfigList* loadConfigList)
{
    assert(loadConfigList);
    assert(!tabletOptions.IsLegacy());

    DisableFieldsConfig disableFieldsConfig;
    if (auto status = tabletOptions.GetFromRawJson("%index_config%.disable_fields", &disableFieldsConfig);
        !status.IsOKOrNotFound()) {
        AUTIL_LOG(ERROR, "parse %%index_config%%.disable_fields failed, status[%s]", status.ToString().c_str());
    }
    if (!disableFieldsConfig.needDisable()) {
        return;
    }
    loadConfigList->PushFront(*disableFieldsConfig.CreateLoadConfig());
}

REGISTER_TABLET_DEPLOYER_HOOK(normal, NormalTabletDeployerHook);

} // namespace indexlib::table
