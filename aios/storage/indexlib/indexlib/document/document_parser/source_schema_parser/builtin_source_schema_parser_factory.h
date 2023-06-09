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
#ifndef __INDEXLIB_BUILTIN_SOURCE_SCHEMA_PARSER_FACTORY_H
#define __INDEXLIB_BUILTIN_SOURCE_SCHEMA_PARSER_FACTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/source_group_config.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser_factory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class BuiltinSourceSchemaParserFactory : public SourceSchemaParserFactory
{
public:
    BuiltinSourceSchemaParserFactory();
    ~BuiltinSourceSchemaParserFactory();

public:
    SourceSchemaParser* CreateSourceSchemaParser() override;
    bool Init(const config::IndexPartitionSchemaPtr& schema, const config::SourceGroupConfigPtr& sourceGroupConfig);

private:
    std::vector<std::string> mFieldsName;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltinSourceSchemaParserFactory);
}} // namespace indexlib::document

#endif //__INDEXLIB_BUILTIN_SOURCE_SCHEMA_PARSER_FACTORY_H
