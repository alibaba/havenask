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
#include "indexlib/document/query/query_parser_creator.h"

#include "autil/Log.h"
#include "indexlib/document/query/json_query_parser.h"

using namespace std;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, QueryParserCreator);

QueryParserCreator::QueryParserCreator() {}

QueryParserCreator::~QueryParserCreator() {}

QueryParserPtr QueryParserCreator::Create(const std::string& parserType)
{
    QueryParserPtr ret;
    if (parserType == "json") {
        AUTIL_LOG(INFO, "create raw document JsonQueryParser.");
        ret.reset(new JsonQueryParser);
        return ret;
    }

    AUTIL_LOG(ERROR, "invalid raw document query parser type [%s]", parserType.c_str());
    return ret;
}
}} // namespace indexlib::document
