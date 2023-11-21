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
#include "build_service/builder/KVSortDocConvertor.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <functional>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/Span.h"
#include "indexlib/base/BinaryStringUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/attribute_reference.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/util/ByteSimilarityHasher.h"

using namespace std;

namespace build_service { namespace builder {
BS_LOG_SETUP(builder, KVSortDocConvertor);

KVSortDocConvertor::KVSortDocConvertor() : _dataBuffer(NULL) {}

KVSortDocConvertor::~KVSortDocConvertor() { DELETE_AND_SET_NULL(_dataBuffer); }

bool KVSortDocConvertor::init(const indexlibv2::config::SortDescriptions& sortDesc,
                              const indexlib::config::IndexPartitionSchemaPtr& schema)
{
    if (schema->GetRegionCount() > 1 && !sortDesc.empty()) {
        BS_LOG(ERROR, "not support multi region kv table sort build by certain fields.");
        return false;
    }

    const indexlib::config::IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    auto kvIndexConfig = DYNAMIC_POINTER_CAST(indexlib::config::KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(kvIndexConfig);
    const auto& valueConfig = kvIndexConfig->GetValueConfig();
    if (!valueConfig->IsSimpleValue()) {
        indexlib::config::PackAttributeConfigPtr packAttrConfig = valueConfig->CreatePackAttributeConfig();
        assert(packAttrConfig);
        _packAttrFormatter.reset(new indexlib::common::PackAttributeFormatter);
        if (!_packAttrFormatter->Init(packAttrConfig)) {
            BS_LOG(ERROR, "init pack attribute formatter fail.");
            return false;
        }
        _plainFormatEncoder.reset(indexlib::common::PackAttributeFormatter::CreatePlainFormatEncoder(packAttrConfig));
        _attrConvertor.reset(
            indexlib::common::AttributeConvertorFactory::GetInstance()->CreatePackAttrConvertor(packAttrConfig));

        auto attrSchema = schema->GetAttributeSchema();
        _convertFuncs.resize(sortDesc.size(), nullptr);
        for (size_t i = 0; i < sortDesc.size(); i++) {
            const auto& sortDescription = sortDesc[i];
            indexlib::common::AttributeReference* ref =
                _packAttrFormatter->GetAttributeReference(sortDescription.GetSortFieldName());
            if (!ref) {
                BS_LOG(ERROR, "Get attribute reference for sort field [%s] fail.",
                       sortDescription.GetSortFieldName().c_str());
                return false;
            }
            auto attrConfig = attrSchema->GetAttributeConfig(sortDescription.GetSortFieldName());
            if (!attrConfig || !attrConfig->GetFieldConfig()->SupportSort()) {
                BS_LOG(ERROR, "sort field [%s] can not be sort field.", sortDescription.GetSortFieldName().c_str());
                return false;
            }
            _convertFuncs[i] = indexlibv2::index::SortValueConvertor::GenerateConvertor(
                sortDescription.GetSortPattern(), attrConfig->GetFieldType());
            if (_convertFuncs[i] == NULL) {
                BS_LOG(ERROR, "init emit key function failed.");
                return false;
            }
            _sortAttrRefs.push_back(ref);
        }
    } else {
        BS_LOG(WARN, "simple value typed KV table,  enable sort build is useless.");
    }
    _dataBuffer = new autil::DataBuffer();
    return true;
}

bool KVSortDocConvertor::convert(const indexlib::document::DocumentPtr& doc, SortDocument& sortDoc)
{
    auto kvDoc = DYNAMIC_POINTER_CAST(indexlib::document::KVDocument, doc);
    if (!kvDoc) {
        return false;
    }
    if (kvDoc->GetDocumentCount() != 1u) {
        return false;
    }

    auto& indexDoc = kvDoc->back();
    sortDoc._docType = indexDoc.GetDocOperateType();
    indexlib::index::PKeyType pkey = indexDoc.GetPKeyHash();
    sortDoc._pk = autil::MakeCString((const char*)&pkey, sizeof(pkey), _pool.get());

    if (sortDoc._docType == ADD_DOC) {
        auto value = indexDoc.GetValue();
        if (_attrConvertor) {
            value = _attrConvertor->Decode(value).data;
        }
        std::string sortKey;
        if (_sortAttrRefs.empty()) { // use sim hash
            uint32_t simHash = 0;
            if (_plainFormatEncoder) {
                simHash = _plainFormatEncoder->GetEncodeSimHash(value);
            } else {
                simHash = indexlib::util::ByteSimilarityHasher::GetSimHash(value.data(), value.size());
            }
            sortKey = indexlibv2::BinaryStringUtil::toInvertString(simHash, false);
        } else {
            size_t headerSize = indexlib::common::VarNumAttributeFormatter::GetEncodedCountFromFirstByte(*value.data());
            const char* baseAddr = value.data() + headerSize;
            for (size_t i = 0; i < _sortAttrRefs.size(); i++) {
                auto dataValue = _sortAttrRefs[i]->GetDataValue(baseAddr);
                assert(_convertFuncs[i] != nullptr);
                sortKey += _convertFuncs[i](dataValue.valueStr, true);
            }
        }
        sortDoc._sortKey = autil::MakeCString(sortKey, _pool.get());
    }
    _dataBuffer->write(kvDoc);
    sortDoc._docStr = autil::MakeCString(_dataBuffer->getData(), _dataBuffer->getDataLen(), _pool.get());
    _dataBuffer->clear();
    return true;
}

}} // namespace build_service::builder
