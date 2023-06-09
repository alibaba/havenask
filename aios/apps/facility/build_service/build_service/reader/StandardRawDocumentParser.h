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
#ifndef ISEARCH_BS_STANDARDRAWDOCUMENTPARSER_H
#define ISEARCH_BS_STANDARDRAWDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentParser.h"
#include "build_service/reader/Separator.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class StandardRawDocumentParser : public RawDocumentParser
{
public:
    StandardRawDocumentParser(const std::string& fieldSep, const std::string& keyValueSep);
    ~StandardRawDocumentParser();

private:
    StandardRawDocumentParser(const StandardRawDocumentParser&);
    StandardRawDocumentParser& operator=(const StandardRawDocumentParser&);

public:
    bool parse(const std::string& docString, document::RawDocument& rawDoc) override;
    bool parseMultiMessage(const std::vector<indexlib::document::RawDocumentParser::Message>& msgs,
                           indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }

private:
    void trim(const char*& ptr, size_t& len);
    std::pair<const char*, size_t> findNext(const Separator& sep, const char*& docCursor, const char* docEnd);

private:
    Separator _fieldSep;
    Separator _keyValueSep;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(StandardRawDocumentParser);

}} // namespace build_service::reader

#endif // ISEARCH_BS_STANDARDRAWDOCUMENTPARSER_H
