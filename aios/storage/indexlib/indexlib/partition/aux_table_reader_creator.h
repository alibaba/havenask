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
#ifndef __INDEXLIB_AUX_TABLE_READER_CREATOR_H
#define __INDEXLIB_AUX_TABLE_READER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/aux_table_reader_typed.h"
#include "indexlib/partition/index_partition_reader.h"

namespace indexlib { namespace partition {

class AuxTableReaderCreator
{
public:
    AuxTableReaderCreator() = default;
    ~AuxTableReaderCreator() = default;

public:
    template <typename T>
    static AuxTableReaderTyped<T>* Create(const IndexPartitionReaderPtr& indexPartReader, const std::string& attrName,
                                          autil::mem_pool::Pool* pool, const std::string& regionName = "")
    {
        const auto& schema = indexPartReader->GetSchema();
        TableType tableType = schema->GetTableType();
        if (tableType == tt_index) {
            const auto& attrReader = indexPartReader->GetAttributeReader(attrName);
            if (!attrReader) {
                return nullptr;
            }
            const index::PrimaryKeyIndexReaderPtr& pkReaderPtr = indexPartReader->GetPrimaryKeyReader();
            if (!pkReaderPtr) {
                return nullptr;
            }
            index::AttributeIteratorBase* iteratorBase = attrReader->CreateIterator(pool);
            typedef index::AttributeIteratorTyped<T> IteratorType;
            IteratorType* iterator = dynamic_cast<IteratorType*>(iteratorBase);
            if (!iterator) {
                IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iteratorBase);
                return nullptr;
            }
            AuxTableReaderImpl<T> impl(pkReaderPtr.get(), iterator);
            return IE_POOL_COMPATIBLE_NEW_CLASS(pool, AuxTableReaderTyped<T>, impl, pool);
        } else if (tableType == tt_kv) {
            regionid_t regionId = DEFAULT_REGIONID;
            if (!regionName.empty()) {
                regionId = schema->GetRegionId(regionName);
            }
            const auto& attrSchema = schema->GetAttributeSchema(regionId);
            if (!attrSchema) {
                return nullptr;
            }
            const auto& attrConfig = attrSchema->GetAttributeConfig(attrName);
            if (!attrConfig) {
                return nullptr;
            }
            const index::KVReaderPtr& kvReader = indexPartReader->GetKVReader(regionId);
            if (!kvReader) {
                return nullptr;
            }
            auto fieldType = attrConfig->GetFieldType();
            auto isMulti = attrConfig->IsMultiValue();
#define PRS_CASE_MACRO(ft)                                                                                             \
    case ft:                                                                                                           \
        if (!isMulti) {                                                                                                \
            typedef typename config::FieldTypeTraits<ft>::AttrItemType T2;                                             \
            if (!std::is_same<T2, T>::value) {                                                                         \
                return nullptr;                                                                                        \
            }                                                                                                          \
        } else {                                                                                                       \
            typedef autil::MultiValueType<typename config::FieldTypeTraits<ft>::AttrItemType> T2;                      \
            if (!std::is_same<T2, T>::value) {                                                                         \
                return nullptr;                                                                                        \
            }                                                                                                          \
        }                                                                                                              \
        break;

            switch (fieldType) {
                NUMBER_FIELD_MACRO_HELPER(PRS_CASE_MACRO);
            case ft_string:
                if (!isMulti) {
                    typedef autil::MultiChar T2;
                    if (!std::is_same<T2, T>::value) {
                        return nullptr;
                    }
                } else {
                    typedef autil::MultiString T2;
                    if (!std::is_same<T2, T>::value) {
                        return nullptr;
                    }
                }
                break;
            default:
                return nullptr;
            };
            size_t attrCount = attrSchema->GetAttributeCount();
            AuxTableReaderImpl<T> impl(kvReader.get(), attrCount > 1 ? attrName : "", pool);
            return IE_POOL_COMPATIBLE_NEW_CLASS(pool, AuxTableReaderTyped<T>, impl, pool);
        } else {
            return nullptr;
        }
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AuxTableReaderCreator);
}} // namespace indexlib::partition

#endif //__INDEXLIB_AUX_TABLE_READER_CREATOR_H
