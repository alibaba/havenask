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

KVTabletWriter::KVTabletWriter(const std::shared_ptr<config::TabletSchema>& schema,
                               const config::TabletOptions* options)
    : CommonTabletWriter(schema, options)
{
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

Status KVTabletWriter::RewriteMergeValue(StringView currentValue, StringView newValue, document::KVDocument* doc,
                                         const uint64_t indexNameHash)
{
    indexlibv2::index::PackAttributeFormatter::PackAttributeFields updateFields;
    auto kvReader = GetKVReader(indexNameHash);
    if (!kvReader) {
        auto status = Status::InternalError("can't get kv reader for indexName: [%s]",
                                            kvReader->GetKVIndexConfig()->GetIndexName().c_str());
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto formatter = kvReader->GetPackAttributeFormatter();
    auto ret = formatter->DecodePatchValues((uint8_t*)newValue.data(), newValue.size(), updateFields);
    if (!ret) {
        auto status = Status::InternalError("decode patch values failed key = [%s], keyHash = [%ld]",
                                            doc->GetPkFieldValue(), doc->GetPKeyHash());
        TABLET_LOG(WARN, "%s", status.ToString().c_str());
        return status;
    }
    auto mergeValue = formatter->MergeAndFormatUpdateFields(currentValue.data(), updateFields,
                                                            /*hasHashKeyInAttrFields*/ true, *_memBuffer);

    doc->SetValue(mergeValue); // TODO(xinfei.sxf) use multi memBuffer to reduce the cost of copy
    doc->SetDocOperateType(ADD_DOC);

    return Status::OK();
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
    auto key = doc->GetPKeyHash();
    auto newValue = doc->GetValue();
    auto indexNameHash = doc->GetIndexNameHash();

    if (unlikely(_tabletReader == nullptr)) {
        // lazy open for offline.
        auto status = OpenTabletReader();
        if (!status.IsOK()) {
            return status;
        }
    }

    // has been delete by previous doc in same batch
    if (_deleteSet.find(key) != _deleteSet.end()) {
        TABLET_LOG(WARN, "raw record is not existed for update field message, drop it, key = [%s], keyHash = [%ld]",
                   doc->GetPkFieldValue().to_string().c_str(), key);
        kvDocBatch->DropDoc(docIdx);
        return Status::OK();
    }

    // handle with fixed len case
    auto result = GetTypeId(indexNameHash);
    if (!result.is_ok()) {
        auto status = Status::InternalError("get type id failed, indexNameHash: [%ld]", indexNameHash);
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
    }
    auto typeId = result.get();
    if (!typeId.isVarLen) {
        return RewriteFixedLenDocument(kvDocBatch, doc, docIdx);
    }

    autil::StringView currentValue;
    auto iter = _currentValue.find(key);

    // find previous doc of same key in same batch
    if (iter != _currentValue.end()) {
        currentValue = iter->second;
        if (!currentValue.empty()) {
            auto attrConvertor = GetAttrConvertor(indexNameHash);
            if (!attrConvertor) {
                return Status::InternalError("get attr convertor, indexNameHash: [%ld]", indexNameHash);
            }
            auto meta = attrConvertor->Decode(currentValue);
            currentValue = meta.data;
            size_t encodeCountLen = 0;
            MultiValueFormatter::decodeCount(currentValue.data(), encodeCountLen);
            currentValue = StringView(currentValue.data() + encodeCountLen, currentValue.size() - encodeCountLen);
        }
        return RewriteMergeValue(currentValue, newValue, doc, indexNameHash);
    }

    // get key-value pair from kv reader
    index::KVReadOptions options;
    options.pool = doc->GetPool();
    options.timestamp = (uint64_t)autil::TimeUtility::currentTime();
    auto kvReader = GetKVReader(indexNameHash);
    if (!kvReader) {
        auto status = Status::InternalError("get kv reader failed, indexNameHash: [%ld]", indexNameHash);
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    auto status = kvReader->Get(key, currentValue, options);
    if (status == KVResultStatus::FOUND) {
        return RewriteMergeValue(currentValue, newValue, doc, indexNameHash);
    } else if (status == KVResultStatus::FAIL) {
        auto status = Status::IOError("failed to find key for update field message, key = [%s], keyHash = [%ld]",
                                      doc->GetPkFieldValue().to_string().c_str(), key);
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    } else {
        TABLET_LOG(WARN, "raw record is not existed for update field message, drop it, key = [%s], keyHash = [%ld]",
                   doc->GetPkFieldValue().to_string().c_str(), key);
        kvDocBatch->DropDoc(docIdx);
    }
    return Status::OK();
}

Status KVTabletWriter::RewriteDocument(document::IDocumentBatch* batch)
{
    auto kvDocBatch = dynamic_cast<document::KVDocumentBatch*>(batch);
    if (kvDocBatch == nullptr) {
        return Status::InternalError("only support KVDocumentBatch for kv index");
    }

    _deleteSet.clear();
    _currentValue.clear();
    for (size_t i = 0; i < kvDocBatch->GetBatchSize(); ++i) {
        if (kvDocBatch->IsDropped(i)) {
            continue;
        }
        auto kvDoc = (document::KVDocument*)(*kvDocBatch)[i].get();
        auto operateType = kvDoc->GetDocOperateType();
        if (operateType == UPDATE_FIELD) {
            IncreaseupdateCountValue(1);
            auto status = RewriteUpdateFieldDocument(kvDocBatch, kvDoc, i);
            if (!status.IsOK()) {
                IncreaseupdateFailedCountValue(1);
                return status;
            } else {
                if (!kvDocBatch->IsDropped(i)) {
                    _currentValue[kvDoc->GetPKeyHash()] = kvDoc->GetValue();
                    _deleteSet.erase(kvDoc->GetPKeyHash());
                } else {
                    IncreaseunexpectedUpdateCountValue(1);
                }
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
    auto status = RewriteDocument(batch.get());
    if (!status.IsOK()) {
        return status;
    }
    return CommonTabletWriter::DoBuild(batch);
}

} // namespace indexlibv2::table
