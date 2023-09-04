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
#include "indexlib/document/kkv/KKVDocumentBatch.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/table/common/CommonTabletWriter.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/kkv_table/KKVTabletReader.h"
#include "indexlib/util/MemBuffer.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::table {

struct KKVStatusMachine {
    std::set<std::pair<uint64_t, uint64_t>> deleteSet;
    std::map<std::pair<uint64_t, uint64_t>, autil::StringView> currentValueMap;
    std::set<uint64_t> maskPkeyIndex;
};

class KKVTabletWriter : public table::CommonTabletWriter
{
public:
    KKVTabletWriter(const std::shared_ptr<config::ITabletSchema>& schema, const config::TabletOptions* options);
    ~KKVTabletWriter();

protected:
    Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                  const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions) override;
    Status DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch) override;

private:
    virtual std::shared_ptr<KKVTabletReader> CreateKKVTabletReader();

private:
    Status RewriteMergeValue(const autil::StringView& currentValue, const autil::StringView& newValue,
                             document::KKVDocument* doc, const uint64_t indexNameHash);
    Status OpenTabletReader();
    void RegisterTableSepecificMetrics() override;
    void ReportTableSepecificMetrics(const kmonitor::MetricsTags& tags) override;
    Status CheckAndRewriteDocument(document::IDocumentBatch* batch);
    Status RewriteUpdateFieldDocument(document::KKVDocumentBatch* kkvDocBatch, document::KKVDocument* doc,
                                      int64_t docIdx);
    Status RewriteUpdateFieldDocumentUseReader(document::KKVDocumentBatch* kkvDocBatch, document::KKVDocument* doc,
                                               int64_t docIdx);
    Status FastRewriteUpdateFieldDocument(document::KKVDocumentBatch* kkvDocBatch, document::KKVDocument* doc,
                                          int64_t docIdx);
    Status FastRewriteDocument(document::KKVDocumentBatch* kkvDocBatch);
    void ClearPkeyInContainer(uint64_t pkey);

    std::shared_ptr<KKVReader> GetKKVReader(uint64_t indexNameHash) const
    {
        if (_kkvReaders.size() == 1 && indexNameHash == 0) {
            return _kkvReaders.begin()->second;
        }
        if (auto it = _kkvReaders.find(indexNameHash); it != _kkvReaders.end()) {
            return it->second;
        }
        return nullptr;
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
    std::shared_ptr<indexlibv2::index::PackAttributeFormatter> GetPackAttributeFormatter(uint64_t indexNameHash) const
    {
        if (_formatterMap.size() == 1 && indexNameHash == 0) {
            return _formatterMap.begin()->second;
        }
        if (auto it = _formatterMap.find(indexNameHash); it != _formatterMap.end()) {
            return it->second;
        }
        return nullptr;
    }

private:
    static const size_t PACK_ATTR_BUFF_INIT_SIZE = 256 * 1024; // 256KB
    std::set<std::pair<uint64_t, uint64_t>> _deleteSet;
    std::map<std::pair<uint64_t, uint64_t>, autil::StringView> _currentValue;
    std::set<uint64_t> _maskPkeyIndex;

    std::map<uint64_t, std::shared_ptr<indexlibv2::index::AttributeConvertor>> _attrConvertorMap;
    std::map<uint64_t, std::shared_ptr<indexlibv2::index::PackAttributeFormatter>> _formatterMap;
    indexlib::util::SimplePool _simplePool;
    indexlib::util::MemBufferPtr _memBuffer;
    std::shared_ptr<KKVTabletReader> _tabletReader;
    std::map<uint64_t, std::shared_ptr<KKVReader>> _kkvReaders;
    // metric
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, updateCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, updateFailedCount);
    INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, unexpectedUpdateCount);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
