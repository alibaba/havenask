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
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser_factory_wrapper.h"

#include "indexlib/config/field_config.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/document/document_parser/source_schema_parser/builtin_source_schema_parser_factory.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser_factory.h"

using namespace std;

using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SourceSchemaParserFactoryWrapper);

SourceSchemaParserFactoryWrapper::SourceSchemaParserFactoryWrapper(
    const config::IndexPartitionSchemaPtr& schema, const config::SourceGroupConfigPtr& sourceGroupConfig)
    : ModuleFactoryWrapper(SourceSchemaParserFactory::FACTORY_NAME)
    , mSchema(schema)
    , mGroupConfig(sourceGroupConfig)
{
}

SourceSchemaParserFactoryWrapper::~SourceSchemaParserFactoryWrapper() {}

SourceSchemaParserFactory* SourceSchemaParserFactoryWrapper::CreateBuiltInFactory() const
{
    BuiltinSourceSchemaParserFactory* factory = new BuiltinSourceSchemaParserFactory();
    if (!factory->Init(mSchema, mGroupConfig)) {
        delete factory;
        return nullptr;
    }
    return factory;
}

SourceSchemaParserPtr SourceSchemaParserFactoryWrapper::CreateSourceSchemaParser() const
{
    if (IsBuiltInFactory()) {
        return SourceSchemaParserPtr(mBuiltInFactory->CreateSourceSchemaParser());
    }

    const SourceSchemaPtr& sourceSchema = mSchema->GetSourceSchema();
    const string& moduleName = mGroupConfig->GetModule();
    ModuleInfo moduleInfo;
    if (!sourceSchema->GetModule(moduleName, moduleInfo)) {
        IE_LOG(ERROR, "get module info failed for module name[%s]", moduleName.c_str());
        return SourceSchemaParserPtr();
    }
    SourceSchemaParserPtr parser(mPluginFactory->CreateSourceSchemaParser());
    if (!parser->Init(moduleInfo.parameters)) {
        IE_LOG(ERROR, "init module[%s] failed", moduleName.c_str());
        return SourceSchemaParserPtr();
    }
    return parser;
}
}} // namespace indexlib::document
