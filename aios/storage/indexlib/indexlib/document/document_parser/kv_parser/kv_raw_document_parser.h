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
#ifndef __INDEXLIB_KV_RAW_DOCUMENT_PARSER_H
#define __INDEXLIB_KV_RAW_DOCUMENT_PARSER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/kv_document/kv_document.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, KVIndexDocument);
DECLARE_REFERENCE_CLASS(document, MultiRegionKVKeyExtractor);

namespace indexlib { namespace document {

class KvRawDocumentParser : public RawDocumentParser
{
public:
    KvRawDocumentParser(const config::IndexPartitionSchemaPtr& schema);

public:
    virtual bool construct(const DocumentInitParamPtr& initParam);

public:
    bool parse(const std::string& docString, document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }
    bool parse(const RawDocumentParser::Message& msg, document::RawDocument& rawDoc) override;
    bool parseMultiMessage(const std::vector<Message>& msgs, document::RawDocument& rawDoc) override;

protected:
    virtual bool parseRawString(const std::string& docString, document::RawDocument& rawDoc);

private:
    bool parseSingleMessage(const Message& msg, document::KVDocument* kvDoc);

protected:
    config::IndexPartitionSchemaPtr mSchema;
    DocumentInitParamPtr mInitParam;
    KvDocumentParserPtr mKvParser;
    std::string mRegionFieldName;
    std::string mUserTimestampFieldName;
    bool mTolerateFieldFormatError;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KvRawDocumentParser);

}} // namespace indexlib::document

#endif
