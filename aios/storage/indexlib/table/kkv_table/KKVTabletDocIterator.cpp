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
#include "indexlib/table/kkv_table/KKVTabletDocIterator.h"

#include "indexlib/document/RawDocument.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kkv/KKVShardRecordIterator.h"
#include "indexlib/index/kkv/common/Trait.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTabletDocIterator);

Status KKVTabletDocIterator::InitFromTabletData(const std::shared_ptr<framework::TabletData>& tabletData)
{
    _tabletSchema = tabletData->GetOnDiskVersionReadSchema();
    auto indexConfigs = _tabletSchema->GetIndexConfigs();
    assert(indexConfigs.size() <= 2);
    std::shared_ptr<config::ValueConfig> pkValueConfig;
    for (const auto& indexConfig : indexConfigs) {
        if (index::KKV_RAW_KEY_INDEX_NAME != indexConfig->GetIndexName()) {
            _kkvIndexConfig = dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
            _valueConfig = _kkvIndexConfig->GetValueConfig();
        } else {
            auto pkValueIndexConfig = dynamic_pointer_cast<config::KVIndexConfig>(indexConfig);
            pkValueConfig = pkValueIndexConfig->GetValueConfig();
        }
    }
    auto status = InitFormatter(_valueConfig, pkValueConfig);
    RETURN_IF_STATUS_ERROR(status, "init formatter failed for indx[%s]", _kkvIndexConfig->GetIndexName().c_str());
    return Status::OK();
}

unique_ptr<index::IShardRecordIterator> KKVTabletDocIterator::CreateShardDocIterator() const
{
    FieldType skeyDictType = _kkvIndexConfig->GetSKeyDictKeyType();
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        using SKeyType = indexlib::index::FieldTypeTraits<type>::AttrItemType;                                         \
        return std::make_unique<index::KKVShardRecordIterator<SKeyType>>();                                            \
    }
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        return nullptr;
    }
    }
}

autil::StringView KKVTabletDocIterator::PreprocessPackValue(const autil::StringView& value)
{
    autil::StringView packValue;
    if (_valueConfig->GetFixedLength() == -1) {
        packValue = value;
        if (_plainFormatEncoder && !_plainFormatEncoder->Decode(packValue, &_pool, packValue)) {
            AUTIL_LOG(ERROR, "decode plain format error.");
        }
    } else {
        assert(!_plainFormatEncoder);
        packValue = value;
    }
    return packValue;
}

} // namespace indexlibv2::table
