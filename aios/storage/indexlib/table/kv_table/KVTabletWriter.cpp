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
#include "indexlib/table/kv_table/KVTabletWriter.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "indexlib/index/kv/ValueExtractorUtil.h"
#include "indexlib/table/kv_table/KVTabletOptions.h"
#include "indexlib/table/kv_table/RewriteDocUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, KVTabletWriter);
#define TABLET_LOG(level, format, args...)                                                                             \
    assert(_tabletData);                                                                                               \
    AUTIL_LOG(level, "[%s] [%p]" format, _tabletData->GetTabletName().c_str(), this, ##args)

KVTabletWriter::KVTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema,
                               const config::TabletOptions* options)
    : CommonTabletWriter(schema, options)
{
    _schemaId = schema->GetSchemaId();
    _memBuffer.reset(new indexlib::util::MemBuffer(PACK_ATTR_BUFF_INIT_SIZE, &_simplePool));
}

std::shared_ptr<KVTabletReader> KVTabletWriter::CreateKVTabletReader()
{
    return std::make_shared<KVTabletReader>(_schema);
}

Status KVTabletWriter::OpenTabletReader()
{
    _tabletReader = CreateKVTabletReader();
    ReadResource readResource;
    auto st = _tabletReader->Open(_tabletData, readResource);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "tablet reader open failed");
        return st;
    }
    auto indexConfigs = _schema->GetIndexConfigs();

    for (const auto& indexConfig : indexConfigs) {
        auto indexName = indexConfig->GetIndexName();
        auto indexReader = _tabletReader->GetIndexReader(indexConfig->GetIndexType(), indexName);
        auto kvReader = std::dynamic_pointer_cast<index::KVIndexReader>(indexReader);
        auto kvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        if (!kvReader || !kvIndexConfig) {
            auto status = Status::InternalError("kv reader dynamic cast failed, indexName: [%s]", indexName.c_str());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }

        if (_schema->GetSchemaId() != kvReader->GetReaderSchemaId()) {
            auto status = Status::InternalError("schema id not equal to kv reader's schema id, indexName: [%s]",
                                                indexName.c_str());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }

        auto indexNameHash = config::IndexConfigHash::Hash(kvIndexConfig);
        std::shared_ptr<indexlibv2::index::AttributeConvertor> attrConvertor;
        std::shared_ptr<indexlibv2::index::PlainFormatEncoder> plainFormatEncoder;
        auto status = ValueExtractorUtil::InitValueExtractor(*kvIndexConfig, attrConvertor, plainFormatEncoder);
        if (!status.IsOK()) {
            return status;
        }
        _kvReaders[indexNameHash] = kvReader;
        _attrConvertorMap[indexNameHash] = attrConvertor;
        _typeIdMap[indexNameHash] = MakeKVTypeId(*kvIndexConfig, nullptr);
    }

    return Status::OK();
}

Status KVTabletWriter::DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                              const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions)
{
    if (_options->IsOnline()) {
        return OpenTabletReader();
    }
    // lazy open tablet reader when updateToAdd happened for offline.
    return Status::OK();
}

void KVTabletWriter::RegisterTableSepecificMetrics()
{
    REGISTER_BUILD_METRIC(updateCount);
    REGISTER_BUILD_METRIC(updateFailedCount);
    REGISTER_BUILD_METRIC(unexpectedUpdateCount);
}

void KVTabletWriter::ReportTableSepecificMetrics(const kmonitor::MetricsTags& tags)
{
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, updateCount);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, updateFailedCount);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, unexpectedUpdateCount);
}

Status KVTabletWriter::RewriteMergeValue(const StringView& currentValue, const StringView& newValue,
                                         document::KVDocument* doc, const uint64_t indexNameHash)
{
    auto kvReader = GetKVReader(indexNameHash);
    if (!kvReader) {
        auto status = Status::InternalError("can't get kv reader for indexName: [%s]",
                                            kvReader->GetKVIndexConfig()->GetIndexName().c_str());
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto formatter = kvReader->GetPackAttributeFormatter();

    return RewriteDocUtil::RewriteMergeValue(currentValue, newValue, formatter, _memBuffer, doc);
}

Status KVTabletWriter::RewriteFixedLenDocument(document::KVDocumentBatch* kvDocBatch, document::KVDocument* doc,
                                               int64_t docIdx)
{
    auto newValue = doc->GetValue();

    // TODO(xinfei.sxf) make difference between miss field and real empty field
    // TODO(xinfei.sxf) check empty value in add case
    if (newValue.empty()) {
        kvDocBatch->DropDoc(docIdx);
    } else {
        doc->SetDocOperateType(ADD_DOC);
    }
    return Status::OK();
}

Status KVTabletWriter::RewriteUpdateFieldDocument(document::KVDocumentBatch* kvDocBatch, document::KVDocument* doc,
                                                  int64_t docIdx)
{
    if (unlikely(_tabletReader == nullptr)) {
        // lazy open for offline.
        auto status = OpenTabletReader();
        if (!status.IsOK()) {
            return status;
        }
    }

    // has been delete by previous doc in same batch
    auto key = doc->GetPKeyHash();
    if (_deleteSet.find(key) != _deleteSet.end()) {
        TABLET_LOG(WARN, "raw record is not existed for update field message, drop it, key = [%s], keyHash = [%ld]",
                   doc->GetPkFieldValue().to_string().c_str(), key);
        kvDocBatch->DropDoc(docIdx);
        return Status::OK();
    }

    // find previous doc of same key in same batch
    autil::StringView currentValue;
    auto iter = _currentValue.find(key);
    if (iter != _currentValue.end()) {
        Status fixedLenStatus;
        if (FixedLenRewriteUpdateFieldDocument(kvDocBatch, doc, docIdx, fixedLenStatus)) {
            return fixedLenStatus;
        }

        currentValue = iter->second;
        auto indexNameHash = doc->GetIndexNameHash();
        if (!currentValue.empty()) {
            auto attrConvertor = GetAttrConvertor(indexNameHash);
            if (!attrConvertor) {
                return Status::InternalError("get attr convertor, indexNameHash: [%ld]", indexNameHash);
            }
            RewriteDocUtil::RewriteValue(attrConvertor, currentValue);
        }
        auto newValue = doc->GetValue();
        return RewriteMergeValue(currentValue, newValue, doc, indexNameHash);
    }

    return RewriteUpdateFieldDocumentUseReader(kvDocBatch, doc, docIdx);
}

Status KVTabletWriter::RewriteUpdateFieldDocumentUseReader(document::KVDocumentBatch* kvDocBatch,
                                                           document::KVDocument* doc, int64_t docIdx)
{
    auto indexNameHash = doc->GetIndexNameHash();
    auto kvReader = GetKVReader(indexNameHash);
    if (!kvReader) {
        auto status = Status::InternalError("get kv reader failed, indexNameHash: [%ld]", indexNameHash);
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    index::KVReadOptions options;
    options.pool = doc->GetPool();
    options.timestamp = (uint64_t)autil::TimeUtility::currentTime();
    autil::StringView currentValue;
    auto status = kvReader->Get(doc->GetPKeyHash(), currentValue, options);
    if (status == KVResultStatus::FOUND) {
        Status fixedLenStatus;
        if (FixedLenRewriteUpdateFieldDocument(kvDocBatch, doc, docIdx, fixedLenStatus)) {
            return fixedLenStatus;
        }
        auto newValue = doc->GetValue();
        return RewriteMergeValue(currentValue, newValue, doc, indexNameHash);
    } else if (status == KVResultStatus::FAIL) {
        auto status = Status::IOError("failed to find key for update field message, key = [%s], keyHash = [%ld]",
                                      doc->GetPkFieldValue().to_string().c_str(), doc->GetPKeyHash());
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    } else {
        TABLET_LOG(WARN, "raw record is not existed for update field message, drop it, key = [%s], keyHash = [%ld]",
                   doc->GetPkFieldValue().to_string().c_str(), doc->GetPKeyHash());
        kvDocBatch->DropDoc(docIdx);
    }
    return Status::OK();
}

bool KVTabletWriter::FixedLenRewriteUpdateFieldDocument(document::KVDocumentBatch* kvDocBatch,
                                                        document::KVDocument* doc, int64_t docIdx, Status& status)
{
    auto indexNameHash = doc->GetIndexNameHash();
    // handle with fixed len case
    auto result = GetTypeId(indexNameHash);
    if (!result.is_ok()) {
        status = Status::InternalError("get type id failed, indexNameHash: [%ld]", indexNameHash);
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return true;
    }
    auto typeId = result.get();
    if (!typeId.isVarLen) {
        status = RewriteFixedLenDocument(kvDocBatch, doc, docIdx);
        return true;
    }
    return false;
}

Status KVTabletWriter::FastRewriteDocument(document::KVDocumentBatch* kvDocBatch)
{
    assert(1 == kvDocBatch->GetBatchSize());
    if (kvDocBatch->IsDropped(0)) {
        return Status::OK();
    }
    auto kvDoc = (document::KVDocument*)(*kvDocBatch)[0].get();
    auto operateType = kvDoc->GetDocOperateType();
    if (operateType == UPDATE_FIELD) {
        if (unlikely(_tabletReader == nullptr)) {
            auto status = OpenTabletReader();
            if (!status.IsOK()) {
                return status;
            }
        }

        IncreaseupdateCountValue(1);
        auto status = RewriteUpdateFieldDocumentUseReader(kvDocBatch, kvDoc, 0);
        if (!status.IsOK()) {
            IncreaseupdateFailedCountValue(1);
            return status;
        } else if (kvDocBatch->IsDropped(0)) {
            IncreaseunexpectedUpdateCountValue(1);
        }
    }
    return Status::OK();
}

Status KVTabletWriter::CheckAndRewriteDocument(document::IDocumentBatch* batch)
{
    auto kvDocBatch = dynamic_cast<document::KVDocumentBatch*>(batch);
    if (kvDocBatch == nullptr) {
        return Status::InternalError("only support KVDocumentBatch for kv index");
    }

    if (kvDocBatch->GetBatchSize() == 1) {
        return FastRewriteDocument(kvDocBatch);
    }

    _deleteSet.clear();
    _currentValue.clear();
    for (size_t i = 0; i < kvDocBatch->GetBatchSize(); ++i) {
        if (kvDocBatch->IsDropped(i)) {
            continue;
        }
        auto kvDoc = (document::KVDocument*)(*kvDocBatch)[i].get();
        if (_schemaId != kvDoc->GetSchemaId()) {
            TABLET_LOG(ERROR, "build kv docuemnt doc failed, doc schema id [%d] not match tablet schmema id [%d]",
                       kvDoc->GetSchemaId(), _schemaId);
            return Status::ConfigError("schema id not match");
        }

        auto operateType = kvDoc->GetDocOperateType();
        if (operateType == UPDATE_FIELD) {
            IncreaseupdateCountValue(1);
            auto status = RewriteUpdateFieldDocument(kvDocBatch, kvDoc, i);
            if (!status.IsOK()) {
                IncreaseupdateFailedCountValue(1);
                return status;
            } else if (!kvDocBatch->IsDropped(i)) {
                _currentValue[kvDoc->GetPKeyHash()] = kvDoc->GetValue();
                _deleteSet.erase(kvDoc->GetPKeyHash());
            } else {
                IncreaseunexpectedUpdateCountValue(1);
            }
        } else if (operateType == ADD_DOC) {
            _currentValue[kvDoc->GetPKeyHash()] = kvDoc->GetValue();
            _deleteSet.erase(kvDoc->GetPKeyHash());
        } else if (operateType == DELETE_DOC) {
            _deleteSet.insert(kvDoc->GetPKeyHash());
            _currentValue.erase(kvDoc->GetPKeyHash());
        }
    }

    return Status::OK();
}

Status KVTabletWriter::DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    auto status = CheckAndRewriteDocument(batch.get());
    if (!status.IsOK()) {
        return status;
    }
    return CommonTabletWriter::DoBuild(batch);
}

} // namespace indexlibv2::table
