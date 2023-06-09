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
#include "indexlib/document/document_parser/source_schema_parser/builtin_source_schema_parser_factory.h"

#include "indexlib/document/document_parser/source_schema_parser/builtin_source_schema_parser.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, BuiltinSourceSchemaParserFactory);

BuiltinSourceSchemaParserFactory::BuiltinSourceSchemaParserFactory() {}

BuiltinSourceSchemaParserFactory::~BuiltinSourceSchemaParserFactory() {}

bool BuiltinSourceSchemaParserFactory::Init(const IndexPartitionSchemaPtr& schema,
                                            const SourceGroupConfigPtr& sourceGroupConfig)
{
    SourceGroupConfig::SourceFieldMode fieldMode = sourceGroupConfig->GetFieldMode();
    switch (fieldMode) {
    case SourceGroupConfig::SourceFieldMode::USER_DEFINE: {
        return false;
    }
    case SourceGroupConfig::SourceFieldMode::ALL_FIELD: {
        const auto& fieldConfigs = schema->GetFieldConfigs();
        for (const auto& fieldConfig : fieldConfigs) {
            mFieldsName.push_back(fieldConfig->GetFieldName());
        }
        return true;
    }
    case SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD: {
        mFieldsName = sourceGroupConfig->GetSpecifiedFields();
        return true;
    }
    default:
        return false;
    }
}

SourceSchemaParser* BuiltinSourceSchemaParserFactory::CreateSourceSchemaParser()
{
    BuiltinSourceSchemaParser* parser = new BuiltinSourceSchemaParser();
    parser->SetFieldsName(mFieldsName);
    return parser;
}
}} // namespace indexlib::document
