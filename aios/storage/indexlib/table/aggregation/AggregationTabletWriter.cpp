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
#include "indexlib/table/aggregation/AggregationTabletWriter.h"

#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/PlainDocument.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/index/aggregation/AggregationIndexFields.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/KVIndexFields.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/table/aggregation/AggregationSchemaUtil.h"
#include "indexlib/table/aggregation/AggregationTabletReader.h"
#include "indexlib/table/aggregation/DeleteGenerator.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, AggregationTabletWriter);

AggregationTabletWriter::AggregationTabletWriter(const std::shared_ptr<config::TabletSchema>& schema,
                                                 const config::TabletOptions* options)
    : CommonTabletWriter(schema, options)
{
}

AggregationTabletWriter::~AggregationTabletWriter() = default;

Status AggregationTabletWriter::DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                                       const framework::BuildResource& buildResource,
                                       const framework::OpenOptions& openOptions)
{
    auto pkIndexConfig = _schema->GetPrimaryKeyIndexConfig();
    if (!pkIndexConfig) {
        return Status::InternalError("no primary key index, can not support insert-or-ignore");
    }
    _pkIndexType = pkIndexConfig->GetIndexType();
    _pkIndexNameHash = config::IndexConfigHash::Hash(pkIndexConfig);
    _tabletReader = std::make_shared<AggregationTabletReader>(_schema);
    auto s = _tabletReader->Open(tabletData, framework::ReadResource());
    if (!s.IsOK()) {
        return s;
    }

    _primaryKeyReader = _tabletReader->GetIndexReader<index::KVIndexReader>(pkIndexConfig->GetIndexType(),
                                                                            pkIndexConfig->GetIndexName());
    if (!_primaryKeyReader) {
        return Status::InternalError("get primary key reader [%s:%s] failed", pkIndexConfig->GetIndexType().c_str(),
                                     pkIndexConfig->GetIndexName().c_str());
    }
    _packAttrFormatter = _primaryKeyReader->GetPackAttributeFormatter();
    auto r = AggregationSchemaUtil::GetVersionField(_schema.get());
    if (!r.IsOK()) {
        return {r.steal_error()};
    }
    auto versionField = std::move(r.steal_value());
    _versionRef = _packAttrFormatter->GetAttributeReference(versionField);
    if (!_versionRef) {
        return Status::InternalError("version field %s does not in primary key index", versionField.c_str());
    }
    if (!indexlib::IsIntegerField(_versionRef->GetFieldType())) {
        return Status::InternalError("version field must be integer type");
    }

    _deleteGenerator = std::make_unique<DeleteGenerator>();
    return _deleteGenerator->Init(_schema, _packAttrFormatter);
}

Status AggregationTabletWriter::DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    auto aggDocBatch = std::dynamic_pointer_cast<document::TemplateDocumentBatch<document::PlainDocument>>(batch);
    if (!aggDocBatch) {
        return Status::InternalError("expect an instance of plain DocumentBatch");
    }
    // only support single document build now to simplify the implementation
    if (aggDocBatch->GetBatchSize() != 1) {
        return Status::InternalError("doc batch size is not 1");
    }

    if (batch->IsDropped(0)) {
        return Status::OK();
    }
    auto plainDoc = aggDocBatch->GetTypedDocument(0);
    auto indexFields = plainDoc->GetIndexFields(_pkIndexType);
    if (!indexFields) {
        return Status::InternalError("get index fields failed [%s]", _pkIndexType.c_str());
    }
    auto pkIndexFields = dynamic_cast<index::KVIndexFields*>(indexFields);
    if (!pkIndexFields) {
        return Status::InternalError("cast to KVIndexFields failed");
    }
    auto docType = pkIndexFields->GetDocOperateType();
    if (docType != ADD_DOC) {
        return CommonTabletWriter::DoBuild(batch);
    }

    autil::mem_pool::UnsafePool pool(128);
    index::KVReadOptions options;
    options.pool = &pool;
    autil::StringView value;
    auto pkSingleField = pkIndexFields->GetSingleField(_pkIndexNameHash);
    if (!pkSingleField) {
        return Status::InternalError("get pk single field failed");
    }
    auto status = _primaryKeyReader->Get(pkSingleField->pkeyHash, value, options);
    if (index::KVResultStatus::FAIL == status || index::KVResultStatus::TIMEOUT == status) {
        return Status::InternalError("check key exist failed");
    } else if (index::KVResultStatus::FOUND == status) {
        auto dataValue = _versionRef->GetDataValue(value.data());
        const auto& lastVersion = dataValue.valueStr;
        auto aggregationIndexFields =
            dynamic_cast<index::AggregationIndexFields*>(plainDoc->GetIndexFields(index::AGGREGATION_INDEX_TYPE_STR));
        if (!aggregationIndexFields) {
            return Status::InternalError("get aggregation index fields failed");
        }
        const auto& currentVersion = aggregationIndexFields->GetVersion();
        if (lastVersion.size() != currentVersion.size()) {
            return Status::InternalError("version invalid");
        }
        if (IsNewerVersion(currentVersion, lastVersion)) {
            auto s = _deleteGenerator->Generate(aggregationIndexFields, value);
            RETURN_STATUS_DIRECTLY_IF_ERROR(s);
        } else {
            // drop
            return Status::OK();
        }
    }
    return CommonTabletWriter::DoBuild(batch);
}

bool AggregationTabletWriter::IsNewerVersion(const autil::StringView& currentVersion,
                                             const autil::StringView& lastVersion) const
{
#define CASE(ft)                                                                                                       \
    case ft: {                                                                                                         \
        using T = typename indexlib::IndexlibFieldType2CppType<ft, false>::CppType;                                    \
        return *reinterpret_cast<const T*>(currentVersion.data()) > *reinterpret_cast<const T*>(lastVersion.data());   \
    }
    switch (_versionRef->GetFieldType()) {
        NUMERIC_FIELD_TYPE_MACRO_HELPER(CASE);
    default:
        return false;
    }
#undef CASE
}

} // namespace indexlibv2::table
