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
#include "suez/drc/SourceUpdate2Add.h"

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/SourceReader.h"
#include "suez/drc/BinaryRecordDecoder.h"
#include "suez/drc/LogRecordBuilder.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, SourceUpdate2Add);

SourceUpdate2Add::SourceUpdate2Add() {}

SourceUpdate2Add::~SourceUpdate2Add() {}

bool SourceUpdate2Add::init(indexlibv2::framework::ITablet *index) {
    auto schema = index->GetTabletSchema();
    if (!schema) {
        return false;
    }

    auto pkConfig = schema->GetPrimaryKeyIndexConfig();
    if (!pkConfig) {
        AUTIL_LOG(ERROR, "table [%s] get primary key index config failed", schema->GetTableName().c_str());
        return false;
    }
    auto pkFields = pkConfig->GetFieldConfigs();
    if (pkFields.size() != 1) {
        AUTIL_LOG(ERROR, "pk has [%lu] fields, expected 1", pkFields.size());
        return false;
    }
    if (schema->GetIndexConfigs(indexlibv2::index::SOURCE_INDEX_TYPE_STR).empty()) {
        AUTIL_LOG(ERROR, "use source update2add but no source config");
        return false;
    }
    _indexName = pkConfig->GetIndexName();
    _indexFieldName = pkFields[0]->GetFieldName();
    _index = index;
    AUTIL_LOG(INFO, "init source update2add rewriter success, pk field [%s]", _indexFieldName.c_str());
    return true;
}

RewriteCode SourceUpdate2Add::rewrite(LogRecord &log) {
    auto type = log.getType();
    if (type == LT_ADD || type == LT_DELETE) {
        return RC_OK;
    } else if (type == LT_ALTER_TABLE) {
        return RC_IGNORE;
    } else if (type != LT_UPDATE) {
        return RC_UNSUPPORTED;
    }
    autil::StringView key;
    if (!log.getField(_indexFieldName, key)) {
        // pk does not exist, just ignore
        return RC_IGNORE;
    }
    if (!_readerSnapshot) {
        return RC_FAIL;
    }
    auto pkReader = std::dynamic_pointer_cast<indexlib::index::PrimaryKeyIndexReader>(
        _readerSnapshot->GetIndexReader(indexlibv2::index::PRIMARY_KEY_INDEX_TYPE_STR, _indexName));
    assert(pkReader);
    auto sourceReader = std::dynamic_pointer_cast<indexlibv2::index::SourceReader>(_readerSnapshot->GetIndexReader(
        indexlibv2::index::SOURCE_INDEX_TYPE_STR, indexlibv2::index::SOURCE_INDEX_NAME));
    assert(sourceReader);
    if (!pkReader || !sourceReader) {
        return RC_FAIL;
    }
    auto docId = pkReader->Lookup(key);
    if (docId == indexlib::INVALID_DOCID) {
        return RC_IGNORE;
    }
    autil::mem_pool::UnsafePool pool(1024);
    indexlib::document::SourceDocument sourceDocument(&pool);
    if (!sourceReader->GetDocument(docId, &sourceDocument).IsOK()) {
        return RC_FAIL;
    }
    std::vector<std::string> fieldNames;
    std::vector<std::string> fieldValues;
    sourceDocument.ExtractFields(fieldNames, fieldValues);
    assert(fieldNames.size() == fieldValues.size());

    LogRecordBuilder builder;
    builder.addField(LogRecord::LOG_TYPE_FIELD, autil::StringView("add"));
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        builder.addField(fieldNames[i], fieldValues[i]);
    }
    autil::StringView haReservedTimestamp;
    if (log.getField(indexlib::HA_RESERVED_TIMESTAMP, haReservedTimestamp)) {
        builder.addField(indexlib::HA_RESERVED_TIMESTAMP, haReservedTimestamp);
    }
    builder.finalize(log);
    return RC_OK;
}

bool SourceUpdate2Add::createSnapshot() {
    _readerSnapshot = _index->GetTabletReader();
    if (!_readerSnapshot) {
        return false;
    }
    return true;
}

void SourceUpdate2Add::releaseSnapshot() { _readerSnapshot.reset(); }

} // namespace suez
