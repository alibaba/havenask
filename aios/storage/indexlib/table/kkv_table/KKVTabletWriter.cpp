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
#include "indexlib/table/kkv_table/KKVTabletWriter.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/IndexConfigHash.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ReadResource.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/ValueExtractorUtil.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/kkv_table/KKVTabletOptions.h"
#include "indexlib/table/kv_table/RewriteDocUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, KKVTabletWriter);
#define TABLET_LOG(level, format, args...)                                                                             \
    assert(_tabletData);                                                                                               \
    AUTIL_LOG(level, "[%s] [%p]" format, _tabletData->GetTabletName().c_str(), this, ##args)

KKVTabletWriter::KKVTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema,
                                 const config::TabletOptions* options)
    : CommonTabletWriter(schema, options)
{
    _memBuffer.reset(new indexlib::util::MemBuffer(PACK_ATTR_BUFF_INIT_SIZE, &_simplePool));
}

KKVTabletWriter::~KKVTabletWriter() {}

std::shared_ptr<KKVTabletReader> KKVTabletWriter::CreateKKVTabletReader()
{
    return std::make_shared<KKVTabletReader>(_schema);
}

Status KKVTabletWriter::OpenTabletReader()
{
    _tabletReader = CreateKKVTabletReader();
    ReadResource readResource;
    auto st = _tabletReader->Open(_tabletData, readResource);
    if (!st.IsOK()) {
        TABLET_LOG(ERROR, "tablet reader open failed");
        return st;
    }
    auto indexConfigs = _schema->GetIndexConfigs();

    for (const auto& indexConfig : indexConfigs) {
        auto indexName = indexConfig->GetIndexName();
        if (KKV_RAW_KEY_INDEX_NAME == indexName) {
            continue;
        }
        auto indexReader = _tabletReader->GetIndexReader(indexConfig->GetIndexType(), indexName);
        auto kkvReader = std::dynamic_pointer_cast<KKVReader>(indexReader);
        auto kkvIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
        if (!kkvReader || !kkvIndexConfig) {
            auto status = Status::InternalError("kkv reader dynamic cast failed, indexName: [%s]", indexName.c_str());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }

        if (_schema->GetSchemaId() != kkvReader->GetReaderSchemaId()) {
            auto status = Status::InternalError("schema id not equal to kkv reader's schema id, indexName: [%s]",
                                                indexName.c_str());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }

        auto indexNameHash = config::IndexConfigHash::Hash(kkvIndexConfig);

        std::shared_ptr<indexlibv2::index::AttributeConvertor> attrConvertor;
        std::shared_ptr<indexlibv2::index::PlainFormatEncoder> plainFormatEncoder;
        {
            auto status =
                index::ValueExtractorUtil::InitValueExtractor(*kkvIndexConfig, attrConvertor, plainFormatEncoder);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "create attrConvertor or plainFormatEncoder fail, indexName[%s]", indexName.c_str());
                return status;
            }
        }

        _kkvReaders[indexNameHash] = kkvReader;
        _attrConvertorMap[indexNameHash] = attrConvertor;

        auto& valueConfig = kkvReader->GetIndexConfig()->GetValueConfig();
        assert(valueConfig);

        auto formatter = std::make_shared<indexlibv2::index::PackAttributeFormatter>();
        auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();

        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "creata pack attribute config fail, indexName[%s]", indexName.c_str());
            return status;
        }

        if (formatter->Init(packAttrConfig)) {
            _formatterMap[indexNameHash] = formatter;
        } else {
            auto status = Status::InternalError("kkv reader dynamic cast failed, indexName: [%s]", indexName.c_str());
            TABLET_LOG(ERROR, "%s", status.ToString().c_str());
            return status;
        }
    }

    return Status::OK();
}

Status KKVTabletWriter::DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                               const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions)
{
    if (_options->IsOnline()) {
        return OpenTabletReader();
    }
    // lazy open tablet reader when updateToAdd happened for offline.
    return Status::OK();
}

void KKVTabletWriter::RegisterTableSepecificMetrics()
{
    REGISTER_BUILD_METRIC(updateCount);
    REGISTER_BUILD_METRIC(updateFailedCount);
    REGISTER_BUILD_METRIC(unexpectedUpdateCount);
}

void KKVTabletWriter::ReportTableSepecificMetrics(const kmonitor::MetricsTags& tags)
{
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, updateCount);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, updateFailedCount);
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS(&tags, unexpectedUpdateCount);
}

Status KKVTabletWriter::RewriteMergeValue(const autil::StringView& currentValue, const autil::StringView& newValue,
                                          document::KKVDocument* doc, const uint64_t indexNameHash)
{
    auto formatter = GetPackAttributeFormatter(indexNameHash);
    return RewriteDocUtil::RewriteMergeValue(currentValue, newValue, formatter, _memBuffer, doc);
}

Status KKVTabletWriter::RewriteUpdateFieldDocumentUseReader(document::KKVDocumentBatch* kkvDocBatch,
                                                            document::KKVDocument* doc, int64_t docIdx)
{
    auto indexNameHash = doc->GetIndexNameHash();
    auto kkvReader = GetKKVReader(indexNameHash);
    if (!kkvReader) {
        auto status = Status::InternalError("get kv reader failed, indexNameHash: [%ld]", indexNameHash);
        TABLET_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    KKVReadOptions options;
    options.pool = doc->GetPool();
    options.timestamp = (uint64_t)autil::TimeUtility::currentTime();
    auto pkey = doc->GetPKeyHash();
    auto skey = doc->GetSKeyHash();
    auto kkvIterator = kkvReader->Lookup(pkey, std::vector<uint64_t> {skey}, options);
    if (kkvIterator && kkvIterator->IsValid()) {
        autil::StringView currentValue;
        kkvIterator->GetCurrentValue(currentValue);
        const auto& newValue = doc->GetValue();
        auto status = RewriteMergeValue(currentValue, newValue, doc, indexNameHash);
        POOL_DELETE_CLASS(kkvIterator);
        return status;
    } else {
        TABLET_LOG(WARN,
                   "raw record is not existed for update field message, drop it, key = [%s], pkeyHash = [%ld] skeyHash "
                   "= [%ld]",
                   doc->GetPkFieldValue().to_string().c_str(), pkey, skey);
        kkvDocBatch->DropDoc(docIdx);
    }
    POOL_DELETE_CLASS(kkvIterator);
    return Status::OK();
}

Status KKVTabletWriter::RewriteUpdateFieldDocument(document::KKVDocumentBatch* kkvDocBatch, document::KKVDocument* doc,
                                                   int64_t docIdx)
{
    if (unlikely(_tabletReader == nullptr)) {
        // lazy open for offline.
        auto status = OpenTabletReader();
        if (!status.IsOK()) {
            return status;
        }
    }

    auto pkeySkeyPair = std::make_pair(doc->GetPKeyHash(), doc->GetSKeyHash());
    if (_deleteSet.find(pkeySkeyPair) != _deleteSet.end()) {
        TABLET_LOG(
            WARN,
            "raw record is drop before update field message, drop it, key = [%s], PkeyHash = [%ld], SkeyHash = [%ld]",
            doc->GetPkFieldValue().to_string().c_str(), pkeySkeyPair.first, pkeySkeyPair.second);
        kkvDocBatch->DropDoc(docIdx);
        return Status::OK();
    }

    auto indexNameHash = doc->GetIndexNameHash();
    auto iter = _currentValue.find(pkeySkeyPair);
    if (iter != _currentValue.end()) {
        autil::StringView currentValue = iter->second;
        if (!currentValue.empty()) {
            auto attrConvertor = GetAttrConvertor(indexNameHash);
            if (!attrConvertor) {
                return Status::InternalError("get attr convertor, indexNameHash: [%ld]", indexNameHash);
            }
            RewriteDocUtil::RewriteValue(attrConvertor, currentValue);
        }
        const auto& newValue = doc->GetValue();
        return RewriteMergeValue(currentValue, newValue, doc, indexNameHash);
    }

    if (_maskPkeyIndex.find(pkeySkeyPair.first) != _maskPkeyIndex.end()) {
        TABLET_LOG(
            WARN,
            "raw record is drop before update field message, drop it, key = [%s], PkeyHash = [%ld], SkeyHash = [%ld]",
            doc->GetPkFieldValue().to_string().c_str(), pkeySkeyPair.first, pkeySkeyPair.second);
        kkvDocBatch->DropDoc(docIdx);
        return Status::OK();
    }

    return RewriteUpdateFieldDocumentUseReader(kkvDocBatch, doc, docIdx);
}

Status KKVTabletWriter::FastRewriteDocument(document::KKVDocumentBatch* kkvDocBatch)
{
    assert(1 == kkvDocBatch->GetBatchSize());

    if (kkvDocBatch->IsDropped(0)) {
        return Status::OK();
    }
    auto kkvDoc = dynamic_cast<document::KKVDocument*>((*kkvDocBatch)[0].get());
    auto operateType = kkvDoc->GetDocOperateType();

    if (operateType == UPDATE_FIELD) {
        IncreaseupdateCountValue(1);
        if (kkvDoc->HasSKey()) {
            if (unlikely(_tabletReader == nullptr)) {
                auto status = OpenTabletReader();
                if (!status.IsOK()) {
                    return status;
                }
            }

            auto status = RewriteUpdateFieldDocumentUseReader(kkvDocBatch, kkvDoc, 0);
            if (!status.IsOK()) {
                IncreaseupdateFailedCountValue(1);
                return status;
            } else if (kkvDocBatch->IsDropped(0)) {
                IncreaseunexpectedUpdateCountValue(1);
            }
        } else {
            IncreaseunexpectedUpdateCountValue(1);
            TABLET_LOG(WARN, "empty skey isn\'t support, drop it, key = [%s], pkeyHash = [%ld]",
                       kkvDoc->GetPkFieldValue().to_string().c_str(), kkvDoc->GetPKeyHash());
            kkvDocBatch->DropDoc(0);
        }
    }

    return Status::OK();
}

void KKVTabletWriter::ClearPkeyInContainer(uint64_t pkey)
{
    auto removeMinPkeySkeyPair = std::make_pair(pkey, (uint64_t)std::numeric_limits<uint64_t>::min);
    auto removeMaxPkeySkeyPair = std::make_pair(pkey, (uint64_t)std::numeric_limits<uint64_t>::max);

    _currentValue.erase(_currentValue.lower_bound(removeMinPkeySkeyPair),
                        _currentValue.upper_bound(removeMaxPkeySkeyPair));

    _deleteSet.erase(_deleteSet.lower_bound(removeMinPkeySkeyPair), _deleteSet.upper_bound(removeMaxPkeySkeyPair));
}

Status KKVTabletWriter::CheckAndRewriteDocument(document::IDocumentBatch* batch)
{
    auto kkvDocBatch = dynamic_cast<document::KKVDocumentBatch*>(batch);
    if (kkvDocBatch == nullptr) {
        return Status::InternalError("only support KKVDocumentBatch for kv index");
    }

    if (kkvDocBatch->GetBatchSize() == 1) {
        return FastRewriteDocument(kkvDocBatch);
    }

    _currentValue.clear();
    _deleteSet.clear();
    _maskPkeyIndex.clear();
    for (size_t i = 0; i < kkvDocBatch->GetBatchSize(); ++i) {
        if (kkvDocBatch->IsDropped(i)) {
            continue;
        }
        auto kkvDoc = dynamic_cast<document::KKVDocument*>((*kkvDocBatch)[i].get());
        auto operateType = kkvDoc->GetDocOperateType();
        auto pkeySkeyPair = std::make_pair(kkvDoc->GetPKeyHash(), kkvDoc->GetSKeyHash());

        if (operateType == UPDATE_FIELD) {
            IncreaseupdateCountValue(1);
            if (kkvDoc->HasSKey()) {
                auto status = RewriteUpdateFieldDocument(kkvDocBatch, kkvDoc, i);
                if (!status.IsOK()) {
                    IncreaseupdateFailedCountValue(1);
                    return status;
                } else {
                    if (!kkvDocBatch->IsDropped(i)) {
                        _currentValue[pkeySkeyPair] = kkvDoc->GetValue();
                    } else {
                        IncreaseunexpectedUpdateCountValue(1);
                    }
                }
            } else {
                IncreaseunexpectedUpdateCountValue(1);
                TABLET_LOG(WARN, "empty skey isn\'t support, drop it, key = [%s], pkeyHash = [%ld]",
                           kkvDoc->GetPkFieldValue().to_string().c_str(), pkeySkeyPair.first);
                kkvDocBatch->DropDoc(i);
            }
        } else if (operateType == ADD_DOC) {
            _currentValue[pkeySkeyPair] = kkvDoc->GetValue();
            _deleteSet.erase(pkeySkeyPair);
        } else if (operateType == DELETE_DOC) {
            if (kkvDoc->HasSKey()) {
                _currentValue.erase(pkeySkeyPair);
                _deleteSet.insert(pkeySkeyPair);
            } else {
                ClearPkeyInContainer(pkeySkeyPair.first);
                _maskPkeyIndex.insert(pkeySkeyPair.first);
            }
        }
    }

    return Status::OK();
}

Status KKVTabletWriter::DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch)
{
    auto status = CheckAndRewriteDocument(batch.get());
    if (!status.IsOK()) {
        return status;
    }
    return CommonTabletWriter::DoBuild(batch);
}

} // namespace indexlibv2::table
