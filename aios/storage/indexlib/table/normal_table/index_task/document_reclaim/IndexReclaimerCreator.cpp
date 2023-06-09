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
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimerCreator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "indexlib/index/inverted_index/InvertedIndexReaderImpl.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/AndIndexReclaimer.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexFieldReclaimer.h"
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexReclaimerParam.h"
namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, IndexReclaimerCreator);

IndexReclaimerCreator::IndexReclaimerCreator(std::shared_ptr<config::TabletSchema> tabletSchema,
                                             std::shared_ptr<framework::TabletData> tabletData,
                                             std::map<segmentid_t, docid_t> segmentId2BaseDocId,
                                             std::map<segmentid_t, size_t> segmentId2DocCount,
                                             const std::shared_ptr<kmonitor::MetricsReporter>& reporter)
    : _tabletSchema(std::move(tabletSchema))
    , _tabletData(std::move(tabletData))
    , _segmentId2BaseDocId(std::move(segmentId2BaseDocId))
    , _segmentId2DocCount(std::move(segmentId2DocCount))
    , _multiFieldIndexReader(std::make_shared<indexlib::index::MultiFieldIndexReader>(nullptr))
    , _metricsReporter(reporter)
{
}

IndexReclaimerCreator::~IndexReclaimerCreator() {}

IndexReclaimer* IndexReclaimerCreator::Create(const IndexReclaimerParam& param) const
{
    const std::string op = param.GetReclaimOperator();
    std::unique_ptr<IndexReclaimer> reclaimer;
    std::vector<std::string> indexNames;
    if (op == IndexReclaimerParam::AND_RECLAIM_OPERATOR) {
        reclaimer.reset(new AndIndexReclaimer(_metricsReporter, param, _segmentId2BaseDocId, _segmentId2DocCount));
        for (const auto& oprand : param.GetReclaimOprands()) {
            indexNames.push_back(oprand.indexName);
        }
    } else {
        assert(op.empty());
        reclaimer.reset(new IndexFieldReclaimer(_metricsReporter, param, _segmentId2BaseDocId, _segmentId2DocCount));
        indexNames.push_back(param.GetReclaimIndex());
    }

    for (auto& indexName : indexNames) {
        auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(
            _tabletSchema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, indexName));
        if (!indexConfig) {
            AUTIL_LOG(ERROR, "reclaim index [%s] not exist", indexName.c_str());
            return nullptr;
        }
        if (!_multiFieldIndexReader->GetIndexReader(indexName)) {
            indexlib::index::InvertedIndexFactory factory;
            index::IndexerParameter indexerParam;
            std::shared_ptr<index::IIndexReader> iReader = factory.CreateIndexReader(indexConfig, indexerParam);
            auto reader = std::dynamic_pointer_cast<indexlib::index::InvertedIndexReader>(iReader);
            if (!reader) {
                AUTIL_LOG(ERROR, "get index reader for [%s] failed", indexName.c_str());
                return nullptr;
            }
            auto status = reader->Open(indexConfig, _tabletData.get());
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "open index reader for [%s] failed", indexName.c_str());
                return nullptr;
            }
            status = _multiFieldIndexReader->AddIndexReader(indexConfig.get(), reader);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "add index reader for [%s] failed", indexName.c_str());
                return nullptr;
            }
        }
    }

    if (!reclaimer->Init(_multiFieldIndexReader)) {
        return nullptr;
    }
    return reclaimer.release();
}
} // namespace indexlibv2::table
