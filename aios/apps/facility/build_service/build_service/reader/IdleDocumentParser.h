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
#pragma once

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document_parser.h"

namespace build_service { namespace reader {

class IdleDocumentParser : public indexlib::document::RawDocumentParser
{
public:
    IdleDocumentParser(const std::string& fieldName);
    ~IdleDocumentParser();

private:
    IdleDocumentParser(const IdleDocumentParser&);
    IdleDocumentParser& operator=(const IdleDocumentParser&);

public:
    bool parse(const std::string& docString, indexlib::document::RawDocument& rawDoc) override;
    bool parseMultiMessage(const std::vector<indexlib::document::RawDocumentParser::Message>& msgs,
                           indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }

private:
    std::string _fieldName;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IdleDocumentParser);

}} // namespace build_service::reader
