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
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser_factory_group.h"

#include "indexlib/plugin/plugin_manager.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::plugin;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SourceSchemaParserFactoryGroup);

SourceSchemaParserFactoryGroup::SourceSchemaParserFactoryGroup() {}

SourceSchemaParserFactoryGroup::~SourceSchemaParserFactoryGroup() {}

bool SourceSchemaParserFactoryGroup::Init(const IndexPartitionSchemaPtr schema, const string& pluginPath)
{
    mPluginPath = pluginPath;
    mSchema = schema;
    const SourceSchemaPtr& sourceSchema = mSchema->GetSourceSchema();
    PluginManagerPtr pluginManager(new PluginManager(pluginPath));
    for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
        const SourceGroupConfigPtr& groupConfig = *iter;
        index::groupid_t groupId = groupConfig->GetGroupId();
        ModuleInfo moduleInfo;
        if (!sourceSchema->GetModule(groupConfig->GetModule(), moduleInfo)) {
            // use builting parser
            assert(groupConfig->GetFieldMode() == SourceGroupConfig::SourceFieldMode::ALL_FIELD ||
                   groupConfig->GetFieldMode() == SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD);
            SourceSchemaParserFactoryWrapperPtr factory(new SourceSchemaParserFactoryWrapper(mSchema, groupConfig));
            if (!factory->Init(pluginManager, "")) {
                IE_LOG(ERROR, "init builting source schema parser factory failed for group[%d].", groupId)
                return false;
            }
            mFactoryWrappers.push_back(factory);
            IE_LOG(INFO, "add builtin source schema parser for group[%d]", groupId);
            continue;
        }

        if (!pluginManager->addModule(moduleInfo)) {
            IE_LOG(ERROR, "init user-define source schema parser factory failed for group[%d].", groupId);
            return false;
        }
        // user-define factory
        SourceSchemaParserFactoryWrapperPtr factory(new SourceSchemaParserFactoryWrapper(mSchema, *iter));
        if (!factory->Init(pluginManager, groupConfig->GetModule())) {
            IE_LOG(ERROR, "init user-define source schema parser factory failed for group[%d].", groupId);
            return false;
        }
        IE_LOG(INFO, "add user-define source schema parser for group[%d]", groupId);
        mFactoryWrappers.push_back(factory);
    }
    IE_LOG(INFO, "init SourceSchemaParserFactoryGroup success.");
    return true;
}

SourceSchemaParserFactoryGroup::SourceSchemaParserGroup SourceSchemaParserFactoryGroup::CreateParserGroup() const
{
    SourceSchemaParserFactoryGroup::SourceSchemaParserGroup group;
    for (auto& factory : mFactoryWrappers) {
        SourceSchemaParserPtr parser = factory->CreateSourceSchemaParser();
        IE_LOG(INFO, "create source schema parser[%lu]", group.size());
        group.push_back(parser);
    }
    return group;
}
}} // namespace indexlib::document
