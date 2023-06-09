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
#ifndef ISEARCH_BS_COMBINEDRAWDOCUMENTPARSER_H
#define ISEARCH_BS_COMBINEDRAWDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document_parser.h"

namespace build_service { namespace reader {

class CombinedRawDocumentParser : public indexlib::document::RawDocumentParser
{
public:
    CombinedRawDocumentParser(const std::vector<indexlib::document::RawDocumentParserPtr>& parsers)
        : _parsers(parsers)
        , _lastParser(0)
        , _parserCount(parsers.size())
    {
    }
    ~CombinedRawDocumentParser() {}

private:
    CombinedRawDocumentParser(const CombinedRawDocumentParser&);
    CombinedRawDocumentParser& operator=(const CombinedRawDocumentParser&);

public:
    bool parse(const std::string& docString, indexlib::document::RawDocument& rawDoc) override;
    bool parseMultiMessage(const std::vector<indexlib::document::RawDocumentParser::Message>& msgs,
                           indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }

private:
    std::vector<indexlib::document::RawDocumentParserPtr> _parsers;
    size_t _lastParser;
    size_t _parserCount;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CombinedRawDocumentParser);

}} // namespace build_service::reader

#endif // ISEARCH_BS_COMBINEDRAWDOCUMENTPARSER_H
