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
#include "ha3/sql/ops/scan/AggIndexKeyCollector.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/table/BuiltinDefine.h"
#include "ha3/sql/ops/scan/Collector.h"


namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, AggIndexKeyCollector);

AggIndexKeyCollector::AggIndexKeyCollector() {}

AggIndexKeyCollector::~AggIndexKeyCollector() {}

bool AggIndexKeyCollector::init(const std::string &indexName,
                                const std::shared_ptr<indexlibv2::config::ITabletSchema> &tableSchema,
                                const matchdoc::MatchDocAllocatorPtr &mountedAllocator)
{
    assert(tableSchema);
    assert(mountedAllocator);

    const auto &tableType = tableSchema->GetTableType();
    if (tableType != indexlib::table::TABLE_TYPE_AGGREGATION) {
        SQL_LOG(ERROR, "not support table type [%s]", tableType.c_str());
        return false;
    }

    auto indexConfig = tableSchema->GetIndexConfig(indexlibv2::index::AGGREGATION_INDEX_TYPE_STR, indexName);
    if (indexConfig == nullptr) {
        SQL_LOG(ERROR, "get index config failed");
        return false;
    }

    const auto &keyFields = indexConfig->GetFieldConfigs();

    for (size_t i = 0; i < keyFields.size(); i++) {
        const auto &field = keyFields[i];
        FieldType fieldType = field->GetFieldType();
        bool isMulti = field->IsMultiValue();
        if (isMulti) {
            SQL_LOG(ERROR, "not support multi value key now");
            return false;
        }
        const std::string &fieldName = field->GetFieldName();
        matchdoc::ReferenceBase *fieldRef = CollectorUtil::declareReference(
            mountedAllocator, fieldType, isMulti, fieldName, CollectorUtil::MOUNT_TABLE_GROUP);
        
        if (!fieldRef) {
            SQL_LOG(ERROR, "declare reference for [%s] failed", fieldName.c_str());
            return false;
        }
        fieldRef->setSerializeLevel(SL_ATTRIBUTE);
        _refs.push_back(fieldRef);
        _types.push_back(fieldType);
    }

    return true;
}

size_t AggIndexKeyCollector::count() const {
    return _refs.size();
}

matchdoc::ReferenceBase* AggIndexKeyCollector::getRef(size_t idx) const{
    return _refs[idx];
}

FieldType AggIndexKeyCollector::getFieldType(size_t idx) const {
    return _types[idx];
}


}
}

