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
#include "suez/drc/Update2Add.h"

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "suez/drc/BinaryRecordDecoder.h"
#include "suez/drc/LogRecordBuilder.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, Update2Add);

Update2Add::Update2Add() {}

Update2Add::~Update2Add() {}

bool Update2Add::init(indexlibv2::framework::ITablet *index) {
    auto schema = index->GetTabletSchema();
    if (!schema) {
        return false;
    }

    auto pkConfig = schema->GetPrimaryKeyIndexConfig();
    if (!pkConfig) {
        AUTIL_LOG(ERROR, "table [%s] get primary key index config failed", schema->GetTableName().c_str());
        return false;
    }
    auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(pkConfig);
    if (!kvIndexConfig) {
        AUTIL_LOG(ERROR, "expect kv index config, it is %s actually", pkConfig->GetIndexType().c_str());
        return false;
    }

    _indexFieldName = kvIndexConfig->GetFieldConfig()->GetFieldName();
    _index = index;

    return true;
}

std::unique_ptr<BinaryRecordDecoder>
Update2Add::prepareDecoder(const std::shared_ptr<indexlibv2::framework::ITabletReader> &reader) const {
    if (!reader) {
        AUTIL_LOG(ERROR, "reader is nullptr");
        return nullptr;
    }
    auto schema = reader->GetSchema();
    if (!schema) {
        AUTIL_LOG(ERROR, "get reader schema failed");
        return nullptr;
    }
    auto pkConfig = schema->GetPrimaryKeyIndexConfig();
    if (!pkConfig) {
        AUTIL_LOG(ERROR, "table [%s] get primary key index config failed", schema->GetTableName().c_str());
        return nullptr;
    }
    auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(pkConfig);
    if (!kvIndexConfig) {
        AUTIL_LOG(ERROR, "expect kv index config, it is %s actually", pkConfig->GetIndexType().c_str());
        return nullptr;
    }
    auto decoder = std::make_unique<BinaryRecordDecoder>();
    if (!decoder->init(*kvIndexConfig)) {
        AUTIL_LOG(ERROR, "init decoder failed");
        return nullptr;
    }
    return decoder;
}

RewriteCode Update2Add::rewrite(LogRecord &log) {
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
    autil::mem_pool::UnsafePool pool(1024);
    indexlibv2::index::KVReadOptions opts;
    opts.pool = &pool;
    autil::StringView value;
    auto s = _readerSnapshot->Get(key, value, opts);
    if (s == indexlibv2::index::KVResultStatus::FOUND) {
        auto binaryRecord = _decoder->decode(value, &pool);
        LogRecordBuilder builder;
        builder.addField(LogRecord::LOG_TYPE_FIELD, autil::StringView("add"));
        for (auto i = 0; i < binaryRecord.getFieldCount(); ++i) {
            auto key = binaryRecord.getFieldName(i);
            std::string fieldValue;
            if (!binaryRecord.getFieldValue(i, fieldValue)) {
                return RC_FAIL;
            }
            builder.addField(key, fieldValue);
        }
        autil::StringView haReservedTimestamp;
        if (log.getField(indexlib::HA_RESERVED_TIMESTAMP, haReservedTimestamp)) {
            builder.addField(indexlib::HA_RESERVED_TIMESTAMP, haReservedTimestamp);
        }
        builder.finalize(log);
        return RC_OK;
    } else if (s == indexlibv2::index::KVResultStatus::FAIL) {
        return RC_FAIL;
    } else {
        // not found or deleted
        return RC_IGNORE;
    }
}

bool Update2Add::createSnapshot() {
    auto tabletReader = _index->GetTabletReader();
    if (!tabletReader) {
        return false;
    }
    auto decoder = prepareDecoder(tabletReader);
    if (!decoder) {
        AUTIL_LOG(ERROR, "update decoder failed");
        return false;
    }
    auto indexReader = tabletReader->GetIndexReader(indexlibv2::index::KV_INDEX_TYPE_STR, _indexFieldName);
    auto readerSnapshot = std::dynamic_pointer_cast<indexlibv2::index::KVIndexReader>(indexReader);
    if (!readerSnapshot) {
        return false;
    }

    _decoder = std::move(decoder);
    _readerSnapshot = readerSnapshot;
    return true;
}

void Update2Add::releaseSnapshot() {
    _readerSnapshot.reset();
    _decoder.reset();
}

} // namespace suez
