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
#include "indexlib/table/normal_table/NormalMemSegment.h"

#include "autil/CommonMacros.h"
#include "indexlib/config/BuildOptionConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/attribute/AttributeMemReader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/common/SortValueConvertor.h"
#include "indexlib/table/normal_table/NormalSchemaResolver.h"
#include "indexlib/table/normal_table/SegmentSortDecisionMaker.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalMemSegment);

NormalMemSegment::NormalMemSegment(const config::TabletOptions* options,
                                   const std::shared_ptr<config::TabletSchema>& schema,
                                   const framework::SegmentMeta& segmentMeta)
    : PlainMemSegment(options, schema, segmentMeta)
    , _buildResourceMetricsNode(nullptr)
    , _dumpMemCostPerDoc(0)
{
}

NormalMemSegment::~NormalMemSegment() {}

std::pair<Status, std::shared_ptr<framework::DumpParams>> NormalMemSegment::CreateDumpParams()
{
    if (!SegmentSortDecisionMaker::NeedSortMemSegment(_sortDescriptions, GetSegmentId())) {
        return {Status::OK(), nullptr};
    }
    AUTIL_LOG(INFO, "begin create sort dump param");
    std::vector<std::shared_ptr<index::AttributeMemReader>> readers;
    for (const auto& sortDesc : _sortDescriptions) {
        auto [status, indexer] = GetIndexer(index::ATTRIBUTE_INDEX_TYPE_STR, sortDesc.GetSortFieldName());
        RETURN2_IF_STATUS_ERROR(status, nullptr, "get attribute indexer [%s] failed",
                                sortDesc.GetSortFieldName().c_str());
        auto attributeMemIndexer = std::dynamic_pointer_cast<index::AttributeMemIndexer>(indexer);
        assert(attributeMemIndexer);
        auto attributeMemReader = attributeMemIndexer->CreateInMemReader();
        readers.push_back(attributeMemReader);
        AUTIL_LOG(INFO, "init sort attribute reader:[%s]", sortDesc.GetSortFieldName().c_str());
    }

    auto convertorFuncs = index::SortValueConvertor::GenerateConvertors(_sortDescriptions, _schema);

    // docid is from 0 (local), [0, docCount)
    uint64_t docCount = GetSegmentInfo()->docCount;
    std::vector<std::pair</*sort keys*/ std::string, docid_t>> sortDocs;
    sortDocs.reserve(docCount);
    const int BUF_LEN = 10;
    uint8_t buf[BUF_LEN];
    for (docid_t docId = 0; docId < docCount; ++docId) {
        std::string sortKey;
        for (size_t i = 0; i < readers.size(); ++i) {
            bool isNull = false;
            bool readed = readers[i]->Read(docId, buf, BUF_LEN, isNull);
            if (!readed) {
                AUTIL_LOG(ERROR, "read attribute [%s] docid [%d] failed",
                          _sortDescriptions[i].GetSortFieldName().c_str(), docId);
                return {Status::Unknown("read falied"), nullptr};
            }
            auto [convertFunc, length] = convertorFuncs[i];
            autil::StringView valueStr((char*)buf, length);
            sortKey += convertFunc(valueStr, isNull);
        }
        sortDocs.push_back({sortKey, docId});
    }
    sort(sortDocs.begin(), sortDocs.end());

    auto dumpParams = std::make_shared<index::DocMapDumpParams>();
    dumpParams->new2old.resize(docCount);
    dumpParams->old2new.resize(docCount);

    for (docid_t newDocId = 0; newDocId < docCount; ++newDocId) {
        dumpParams->new2old[newDocId] = sortDocs[newDocId].second;
        dumpParams->old2new[sortDocs[newDocId].second] = newDocId;
    }
    AUTIL_LOG(INFO, "end create sort dump param");
    return {Status::OK(), dumpParams};
}

void NormalMemSegment::CalcMemCostInCreateDumpParams()
{
    if (_sortDescriptions.empty()) {
        return;
    }
    if (unlikely(!_buildResourceMetricsNode)) {
        _buildResourceMetricsNode = _buildResourceMetrics->AllocateNode();
        int64_t sortFieldCost = 0;
        for (const auto& desc : _sortDescriptions) {
            const std::string& fieldName = desc.GetSortFieldName();
            auto indexConfig = _schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
            assert(indexConfig);
            auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);
            assert(attributeConfig);
            FieldType fieldType = attributeConfig->GetFieldType();
            sortFieldCost += indexlib::index::SizeOfFieldType(fieldType);
        }
        // old->new, new->old, vector<{docid, sort fields}>
        _dumpMemCostPerDoc = sizeof(docid_t) * 3 + sortFieldCost;
    }
    _buildResourceMetricsNode->Update(indexlib::util::BMT_DUMP_TEMP_MEMORY_SIZE,
                                      _segmentMeta.segmentInfo->docCount * _dumpMemCostPerDoc);
}

} // namespace indexlibv2::table
