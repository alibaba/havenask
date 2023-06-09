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
#ifndef ISEARCH_BS_SWIFTFIELDFILTERRAWDOCUMENTPARSER_H
#define ISEARCH_BS_SWIFTFIELDFILTERRAWDOCUMENTPARSER_H

#include "build_service/common_define.h"
#include "build_service/reader/RawDocumentParser.h"
#include "build_service/util/Log.h"
#include "swift/common/FieldGroupReader.h"

namespace build_service { namespace reader {

class SwiftFieldFilterRawDocumentParser : public RawDocumentParser
{
public:
    using RawDocumentParser::parse;

public:
    SwiftFieldFilterRawDocumentParser();
    ~SwiftFieldFilterRawDocumentParser();

private:
    SwiftFieldFilterRawDocumentParser(const SwiftFieldFilterRawDocumentParser&);
    SwiftFieldFilterRawDocumentParser& operator=(const SwiftFieldFilterRawDocumentParser&);

public:
    virtual bool parse(const std::string& docString, document::RawDocument& rawDoc) override;
    bool parse(const char* dataStr, size_t dataLen, document::RawDocument& rawDoc);
    bool parseMultiMessage(const std::vector<indexlib::document::RawDocumentParser::Message>& msgs,
                           indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }

private:
    swift::common::FieldGroupReader _fieldGroupReader;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::reader

#endif // ISEARCH_BS_SWIFTFIELDFILTERRAWDOCUMENTPARSER_H
