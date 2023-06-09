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
#ifndef __INDEXLIB_RAW_DOCUMENT_PARSER_H
#define __INDEXLIB_RAW_DOCUMENT_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/IRawDocumentParser.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class RawDocumentParser
{
public:
    using Message = indexlibv2::document::IRawDocumentParser::Message;

public:
    RawDocumentParser() {}
    virtual ~RawDocumentParser() {}

private:
    RawDocumentParser(const RawDocumentParser&);
    RawDocumentParser& operator=(const RawDocumentParser&);

public:
    virtual bool init(const std::map<std::string, std::string>& kvMap) { return true; }

    virtual bool parse(const std::string& docString, document::RawDocument& rawDoc) = 0;
    virtual bool parse(const Message& msg, document::RawDocument& rawDoc) { return parse(msg.data, rawDoc); }
    virtual bool parseMultiMessage(const std::vector<Message>& msgs, document::RawDocument& rawDoc) { return false; }
};

DEFINE_SHARED_PTR(RawDocumentParser);
}} // namespace indexlib::document

#endif //__INDEXLIB_RAW_DOCUMENT_PARSER_H
