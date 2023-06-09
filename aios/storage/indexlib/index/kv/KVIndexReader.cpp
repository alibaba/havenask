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
#include "indexlib/index/kv/KVIndexReader.h"

#include "autil/Log.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KVIndexReader);

KVIndexReader::KVIndexReader(schemaid_t readerSchemaId)
    : _readerSchemaId(readerSchemaId)
    , _keyHasherType(indexlib::hft_unknown)
    , _valueType(index::KVVT_UNKNOWN)
    , _ttl(indexlib::DEFAULT_TIME_TO_LIVE)
{
}

KVIndexReader::~KVIndexReader() {}

Status KVIndexReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const framework::TabletData* tabletData) noexcept
{
    _kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    if (!_kvIndexConfig) {
        return Status::InternalError("%s:%s is not an kv index", indexConfig->GetIndexName().c_str(),
                                     indexConfig->GetIndexType().c_str());
    }
    if (!InitInnerMeta(_kvIndexConfig)) {
        return Status::InternalError("init InnerMeta fail.");
    }

    // TODO open segments
    return DoOpen(_kvIndexConfig, tabletData);
}

bool KVIndexReader::InitInnerMeta(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvConfig)
{
    assert(!kvConfig->GetFieldConfig()->IsMultiValue());
    _keyHasherType = kvConfig->GetKeyHashFunctionType();

    auto& valueConfig = kvConfig->GetValueConfig();
    assert(valueConfig);

    if (valueConfig->GetAttributeCount() == 1) {
        if (!valueConfig->GetAttributeConfig(0)->IsMultiValue() &&
            (valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string)) {
            _valueType = index::KVVT_TYPED;
        } else {
            _valueType = index::KVVT_PACKED_ONE_FIELD;
        }
    } else {
        _valueType = index::KVVT_PACKED_MULTI_FIELD;
    }

    if (kvConfig->GetTTL() != indexlib::INVALID_TTL) {
        _ttl = kvConfig->GetTTL();
    }

    _formatter = std::make_shared<indexlibv2::index::PackAttributeFormatter>();
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "creata pack attribute config fail, indexName[%s]", kvConfig->GetIndexName().c_str());
        return false;
    }
    return _formatter->Init(packAttrConfig);
}

} // namespace indexlibv2::index
