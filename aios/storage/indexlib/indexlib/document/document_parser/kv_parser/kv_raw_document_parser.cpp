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
#include "indexlib/document/document_parser/kv_parser/kv_raw_document_parser.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/document/document_parser/kv_parser/kkv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/kv_document/kv_message_generated.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/misc/error_log_collector.h"

namespace indexlib { namespace document {
IE_LOG_SETUP(document, KvRawDocumentParser);

KvRawDocumentParser::KvRawDocumentParser(const config::IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
    , mUserTimestampFieldName(HA_RESERVED_TIMESTAMP)
    , mTolerateFieldFormatError(false)
{
}

bool KvRawDocumentParser::construct(const DocumentInitParamPtr& initParam)
{
    mInitParam = initParam;
    if (mSchema->GetTableType() == tt_kv) {
        mKvParser = std::make_shared<KvDocumentParser>(mSchema);
    } else if (mSchema->GetTableType() == tt_kkv) {
        mKvParser = std::make_shared<KkvDocumentParser>(mSchema);
    } else {
        assert(false);
        IE_LOG(ERROR, "table [%s] is not kv/kkv", mSchema->GetSchemaName().c_str());
        return false;
    }
    if (initParam) {
        initParam->GetValue("region_field_name", mRegionFieldName);
        initParam->GetValue(TIMESTAMP_FIELD, mUserTimestampFieldName);
        std::string value;
        initParam->GetValue("tolerate_field_format_error", value);
        if (!value.empty()) {
            autil::StringUtil::parseTrueFalse(value, mTolerateFieldFormatError);
        }
    }
    return mKvParser->Init(initParam);
}

bool KvRawDocumentParser::parse(const RawDocumentParser::Message& msg, document::RawDocument& rawDoc)
{
    document::RawDocumentPtr rawDocPtr(rawDoc.createNewDocument());
    auto& kvDoc = dynamic_cast<document::KVDocument&>(rawDoc);
    kvDoc.ReleaseRawDoc();
    return parseSingleMessage(msg, &kvDoc);
}

bool KvRawDocumentParser::parseSingleMessage(const Message& msg, document::KVDocument* kvDoc)
{
    document::RawDocumentPtr rawDocPtr(kvDoc->createNewDocument());
    if (!parseRawString(msg.data, *rawDocPtr)) {
        return false;
    }
    auto extendDoc = std::make_shared<IndexlibExtendDocument>();
    extendDoc->setRawDocument(rawDocPtr);
    regionid_t regionId = DEFAULT_REGIONID;
    if (!mRegionFieldName.empty()) {
        std::string regionField = rawDocPtr->getField(mRegionFieldName);
        if (!regionField.empty()) {
            auto id = mSchema->GetRegionId(regionField);
            if (id != INVALID_REGIONID) {
                regionId = id;
            }
        }
    }
    extendDoc->setRegionId(regionId);
    int64_t userTimestamp = rawDocPtr->getDocTimestamp();
    if (rawDocPtr->exist(mUserTimestampFieldName)) { // usertimestamp
        auto userTimestampStr = rawDocPtr->getField(mUserTimestampFieldName);
        int64_t ts = autil::StringUtil::strToInt64WithDefault(userTimestampStr.c_str(), INVALID_TIMESTAMP);
        if (ts != INVALID_TIMESTAMP) {
            userTimestampStr = ts;
        }
    }
    if (userTimestamp == INVALID_TIMESTAMP) {
        userTimestamp = msg.timestamp;
    }

    bool ret = mKvParser->Parse(extendDoc, kvDoc);
    if (!ret) {
        return false;
    }

    if (!mTolerateFieldFormatError and kvDoc->HasFormatError()) {
        auto pkValue = kvDoc->back().GetPkFieldValue();
        ERROR_COLLECTOR_LOG(ERROR, "Doc[pk=%s] has format error, will be skipped", pkValue.data());
        kvDoc->EraseLastDoc();
        kvDoc->ClearFormatError();
        return false;
    } else {
        kvDoc->back().SetUserTimestamp(userTimestamp);
        kvDoc->back().SetTimestamp(msg.timestamp);
        kvDoc->setDocTimestamp(msg.timestamp);
        kvDoc->MergeRawDoc(static_cast<document::KVDocument*>(rawDocPtr.get()));
    }
    return true;
}

bool KvRawDocumentParser::parseMultiMessage(const std::vector<Message>& msgs, document::RawDocument& rawDoc)
{
    auto& multiDoc = static_cast<document::KVDocument&>(rawDoc);
    multiDoc.ReleaseRawDoc();
    bool hasValidDoc = false;
    for (const auto& msg : msgs) {
        if (!parseSingleMessage(msg, &multiDoc)) {
            continue;
        }
        hasValidDoc = true;
        multiDoc.setDocTimestamp(msg.timestamp);
        multiDoc.SetDocInfo({msg.hashId, msg.timestamp, 0});
    }
    multiDoc.SetDocOperateType(ADD_DOC);

    return hasValidDoc;
}

bool KvRawDocumentParser::parseRawString(const std::string& docString, document::RawDocument& rawDoc)
{
    // TODO: use json
    //    a=1;b=1;c=2;
    auto kvs = autil::StringUtil::split(docString, ";");
    for (const auto& kv : kvs) {
        auto kvpair = autil::StringUtil::split(kv, "=");
        if (kvpair.size() == 2) {
            rawDoc.setField(autil::StringView(kvpair[0].data(), kvpair[0].size()),
                            autil::StringView(kvpair[1].data(), kvpair[1].size()));
        } else if (kvpair.size() == 1) {
            rawDoc.setField(autil::StringView(kvpair[0].data(), kvpair[0].size()), autil::StringView::empty_instance());
        }
        if (kvpair[0] == document::CMD_TAG) {
            rawDoc.setDocOperateType(
                RawDocument::getDocOperateType(autil::StringView(kvpair[1].data(), kvpair[1].size())));
        }
    }
    return true;
}

}} // namespace indexlib::document
