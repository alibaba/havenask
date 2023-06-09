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
#ifndef __INDEXLIB_SOURCE_SCHEMA_PARSER_H
#define __INDEXLIB_SOURCE_SCHEMA_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::document {
class RawDocument;
}

namespace indexlib { namespace document {

class SourceSchemaParser
{
public:
    typedef std::map<std::string, std::string> KeyValueMap;
    DEFINE_SHARED_PTR(KeyValueMap);

public:
    SourceSchemaParser() = default;
    virtual ~SourceSchemaParser() = default;

public:
    virtual bool Init(const KeyValueMap& initParam) = 0;
    virtual bool CreateDocSrcSchema(const std::shared_ptr<indexlibv2::document::RawDocument>& rawDoc,
                                    std::vector<std::string>& sourceSchema) const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceSchemaParser);
}} // namespace indexlib::document

#endif //__INDEXLIB_SOURCE_SCHEMA_PARSER_H
