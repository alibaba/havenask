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
#include "indexlib/table/kkv_table/KKVReaderFactory.h"

#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/table/kkv_table/KKVCachedReaderImpl.h"
#include "indexlib/table/kkv_table/KKVReaderImpl.h"

namespace indexlibv2::table {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.table, KVReader);

std::shared_ptr<KKVReader> KKVReaderFactory::Create(const framework::ReadResource& readResource,
                                                    const std::shared_ptr<config::KKVIndexConfig>& kkvIndexConfig,
                                                    const schemaid_t schemaId)
{
    FieldType skeyDictType = kkvIndexConfig->GetSKeyDictKeyType();
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type: {                                                                                                       \
        using SKeyType = indexlib::index::FieldTypeTraits<type>::AttrItemType;                                         \
        if (readResource.searchCache) {                                                                                \
            auto reader = std::make_shared<KKVCachedReaderImpl<SKeyType>>(schemaId);                                   \
            reader->SetSearchCache(readResource.searchCache);                                                          \
            return reader;                                                                                             \
        } else {                                                                                                       \
            return std::make_shared<KKVReaderImpl<SKeyType>>(schemaId);                                                \
        }                                                                                                              \
    }

        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
    }
    }
    return nullptr;
}

} // namespace indexlibv2::table
