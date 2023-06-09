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
#ifndef __INDEXLIB_SOURCE_SCHEMA_PARSER_FACTORY_H
#define __INDEXLIB_SOURCE_SCHEMA_PARSER_FACTORY_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document_parser/source_schema_parser/source_schema_parser.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/ModuleFactory.h"

namespace indexlib { namespace document {

class SourceSchemaParserFactory : public plugin::ModuleFactory
{
public:
    SourceSchemaParserFactory();
    virtual ~SourceSchemaParserFactory();

public:
    void destroy() override { delete this; }

public:
    virtual SourceSchemaParser* CreateSourceSchemaParser() = 0;

public:
    const static std::string FACTORY_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceSchemaParserFactory);

extern "C" plugin::ModuleFactory* createSourceSchemaParserFactory();
extern "C" void destroySourceSchemaParserFactory(plugin::ModuleFactory* factory);
}} // namespace indexlib::document

#endif //__INDEXLIB_SOURCE_SCHEMA_PARSER_FACTORY_H
