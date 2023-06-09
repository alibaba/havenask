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
#pragma once

#include <vector>

#include "autil/Log.h"
#include "autil/result/Errors.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/kv/KVDocumentBatch.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/table/common/CommonTabletWriter.h"
#include "indexlib/table/kv_table/KVTabletReader.h"
#include "indexlib/util/MemBuffer.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::common {
class AttributeConvertor;
class PlainFormatEncoder;
} // namespace indexlib::common

namespace indexlib::document {
class MultiRegionPackAttributeAppender;
};

namespace indexlibv2::table {

class KVTabletWriter : public table::CommonTabletWriter
{
public:
    KVTabletWriter(const std::shared_ptr<config::TabletSchema>& schema, const config::TabletOptions* options);
    ~KVTabletWriter() = default;

protected:
    Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                  const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions) override;
    Status DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch) override;

private:
    virtual std::shared_ptr<KVTabletReader> CreateKVTabletReader();
    void RegisterTableSepecificMetrics() override;
    void ReportTableSepecificMetrics(const kmonitor::MetricsTags& tags) override;
    Status RewriteDocument(document::IDocumentBatch* batch);
    Status RewriteUpdateFieldDocument(document::KVDocumentBatch* kvDocBatch, document::KVDocument* doc, int64_t docIdx);
    Status RewriteFixedLenDocument(document::KVDocumentBatch* kvDocBatch, document::KVDocument* doc, int64_t docIdx);
    Status RewriteMergeValue(autil::StringView currentValue, autil::StringView newValue, document::KVDocument* doc,
                             const uint64_t indexNameHash);
    Status OpenTabletReader();
    Status InitRawPKDocGenerator();

private:
    static const size_t PACK_ATTR_BUFF_INIT_SIZE = 256 * 1024; // 256KB
    indexlibv2::index::KVIndexReaderPtr GetKVReader(uint64_t indexNameHash) const
    {
        if (_kvReaders.size() == 1 && indexNameHash == 0) {
            return _kvReaders.begin()->second;
        }
        if (auto it = _kvReaders.find(indexNameHash); it != _kvReaders.end()) {
            return it->second;
        }
        return nullptr;
    }
    autil::result::Result<indexlibv2::index::KVTypeId> GetTypeId(uint64_t indexNameHash) const
    {
        if (_typeIdMap.size() == 1 && indexNameHash == 0) {
            return _typeIdMap.begin()->second;
        }
        if (auto it = _typeIdMap.find(indexNameHash); it != _typeIdMap.end()) {
            return it->second;
        }
        return autil::result::RuntimeError::make("not found");
    }
    std::shared_ptr<indexlibv2::index::AttributeConvertor> GetAttrConvertor(uint64_t indexNameHash) const
    {
        if (_attrConvertorMap.size() == 1 && indexNameHash == 0) {
            return _attrConvertorMap.begin()->second;
        }
        if (auto it = _attrConvertorMap.find(indexNameHash); it != _attrConvertorMap.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    indexlib::util::SimplePool _simplePool;
    indexlib::util::MemBufferPtr _memBuffer;
    std::set<uint64_t> _deleteSet;
    std::map<uint64_t, autil::StringView> _currentValue;
    std::shared_ptr<KVTabletReader> _tabletReader;
    std::map<uint64_t, indexlibv2::index::KVIndexReaderPtr> _kvReaders;
    std::map<uint64_t, indexlibv2::index::KVTypeId> _typeIdMap;
    std::map<uint64_t, std::shared_ptr<indexlibv2::index::AttributeConvertor>> _attrConvertorMap;
    // metric
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, updateCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, updateFailedCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, unexpectedUpdateCount);

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
