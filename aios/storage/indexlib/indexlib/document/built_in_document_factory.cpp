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
#include "indexlib/document/built_in_document_factory.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_parser/kv_parser/fb_kv_raw_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kkv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kv_raw_document_parser.h"
#include "indexlib/document/document_parser/line_data_parser/line_data_document_parser.h"
#include "indexlib/document/document_parser/normal_parser/normal_document_parser.h"
#include "indexlib/document/kv_document/kv_document.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, BuiltInDocumentFactory);

const size_t BuiltInDocumentFactory::MAX_KEY_SIZE = 4096;

std::string BuiltInDocumentFactory::MSG_TYPE = "message_type";
std::string BuiltInDocumentFactory::MSG_TYPE_DEFAULT = "default";
std::string BuiltInDocumentFactory::MSG_TYPE_FB = "flatbuffer";

BuiltInDocumentFactory::BuiltInDocumentFactory()
{
    size_t maxKeySize = autil::EnvUtil::getEnv("DEFAULT_RAWDOC_MAX_KEY_SIZE", BuiltInDocumentFactory::MAX_KEY_SIZE);
    mHashKeyMapManager.reset(new KeyMapManager(maxKeySize));
}

BuiltInDocumentFactory::~BuiltInDocumentFactory() {}

RawDocument* BuiltInDocumentFactory::CreateRawDocument(const IndexPartitionSchemaPtr& schema)
{
    if (!schema) {
        return new (std::nothrow) DefaultRawDocument(mHashKeyMapManager);
    }
    TableType tableType = schema->GetTableType();
    if (tableType == tt_kv || tableType == tt_kkv) {
        return new (std::nothrow) document::KVDocument(mHashKeyMapManager);
    } else {
        return new (std::nothrow) DefaultRawDocument(mHashKeyMapManager);
    }
}

RawDocument* BuiltInDocumentFactory::CreateMultiMessageRawDocument(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    TableType tableType = schema->GetTableType();
    if (tableType == tt_kv || tableType == tt_kkv) {
        return new (std::nothrow) document::KVDocument(mHashKeyMapManager);
    } else {
        return nullptr;
    }
}

RawDocumentParser* BuiltInDocumentFactory::CreateRawDocumentParser(const config::IndexPartitionSchemaPtr& schema,
                                                                   const DocumentInitParamPtr& initParam)
{
    if (!schema) {
        IE_LOG(ERROR, "schema is null!");
        return nullptr;
    }

    TableType tableType = schema->GetTableType();
    if (tableType == tt_kv || tableType == tt_kkv) {
        string msgType;
        initParam->GetValue(MSG_TYPE, msgType);
        if (msgType == MSG_TYPE_FB) {
            auto kvParser = new FbKvRawDocumentParser(schema);
            if (!kvParser->construct(initParam)) {
                IE_LOG(ERROR, "construct flatbuffer kv raw document parser for [%s] failed",
                       schema->GetSchemaName().c_str());
            }
            return kvParser;
        } else {
            auto kvParser = new KvRawDocumentParser(schema);
            if (!kvParser->construct(initParam)) {
                IE_LOG(ERROR, "construct kv raw document parser for [%s] failed", schema->GetSchemaName().c_str());
            }
            return kvParser;
        }
    }
    return nullptr;
}

DocumentParser* BuiltInDocumentFactory::CreateDocumentParser(const IndexPartitionSchemaPtr& schema)
{
    if (!schema) {
        IE_LOG(ERROR, "schema is null!");
        return nullptr;
    }

    TableType tableType = schema->GetTableType();
    if (tableType == tt_kv) {
        return new (std::nothrow) KvDocumentParser(schema);
    } else if (tableType == tt_kkv) {
        return new (std::nothrow) KkvDocumentParser(schema);
    } else if (tableType == tt_linedata) {
        return new (std::nothrow) LineDataDocumentParser(schema);
    }

    if (tableType == tt_customized) {
        IE_LOG(WARN, "customized table type not set customized_document_config, "
                     "will use NormalDocumentParser by default!");
    }
    return new (std::nothrow) NormalDocumentParser(schema);
}
}} // namespace indexlib::document
