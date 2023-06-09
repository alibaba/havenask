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
#include "indexlib/table/normal_table/index_task/SortedReclaimMap.h"

#include <queue>

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/inverted_index/truncate/MultiAttributeEvaluator.h"
#include "indexlib/index/inverted_index/truncate/MultiComparator.h"
#include "indexlib/table/normal_table/index_task/WeightedDocIterator.h"
#include "indexlib/util/ClassTypedFactory.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, SortedReclaimMap);

Status SortedReclaimMap::Init(const std::shared_ptr<config::TabletSchema>& schema,
                              const config::SortDescriptions& sortDescs, const SegmentMergePlan& segMergePlan,
                              const std::shared_ptr<framework::TabletData>& tabletData,
                              const SegmentSplitHandler& segmentSplitHandler)
{
    int64_t before = autil::TimeUtility::currentTimeInSeconds();
    std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>> attrConfigs;
    std::vector<config::SortPattern> sortPatterns;
    for (size_t i = 0; i < sortDescs.size(); ++i) {
        const std::string& fieldName = sortDescs[i].GetSortFieldName();
        auto indexConfig = schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
        if (!indexConfig) {
            AUTIL_LOG(ERROR, "Sort Field [%s] not exist in attribute!", fieldName.c_str());
            return Status::Corruption();
        }
        auto attrConfig = std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);
        assert(attrConfig);
        if (attrConfig->IsMultiValue()) {
            AUTIL_LOG(ERROR, "Sort Field [%s] should not be multi value!", fieldName.c_str());
            return Status::Corruption();
        }
        attrConfigs.push_back(attrConfig);
        sortPatterns.push_back(sortDescs[i].GetSortPattern());
    }

    std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>> srcSegments;
    CollectSrcSegments(segMergePlan, tabletData, srcSegments);
    assert(segMergePlan.GetTargetSegmentCount() > 0);
    segmentid_t baseSegmentId = segMergePlan.GetTargetSegmentId(0);
    auto status = SortByWeightInit(segMergePlan.GetTargetSegmentCount(), baseSegmentId, srcSegments, attrConfigs,
                                   sortPatterns, segmentSplitHandler);
    RETURN_IF_STATUS_ERROR(status, "init sort reclaimmap failed");
    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    AUTIL_LOG(INFO, "Init reclaim map use %ld seconds", (after - before));
    return Status::OK();
}

Status SortedReclaimMap::TEST_Init(
    segmentid_t baseSegmentId, const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
    const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
    const std::vector<config::SortPattern>& sortPatterns, const SegmentSplitHandler& segmentSplitHandler)
{
    return SortByWeightInit(0, baseSegmentId, srcSegments, attrConfigs, sortPatterns, segmentSplitHandler);
}

Status SortedReclaimMap::SortByWeightInit(
    size_t targetSegmentCount, segmentid_t baseSegmentId,
    const std::vector<std::pair<docid_t, std::shared_ptr<framework::Segment>>>& srcSegments,
    const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
    const std::vector<indexlibv2::config::SortPattern>& sortPatterns, const SegmentSplitHandler& segmentSplitHandler)
{
    AUTIL_LOG(INFO, "begin sortByWeight init");

    std::priority_queue<indexlib::index::WeightedDocIterator*, std::vector<indexlib::index::WeightedDocIterator*>,
                        indexlib::index::WeightedDocIteratorComp>
        docHeap;
    auto comparator = std::make_shared<indexlib::index::MultiComparator>();
    auto docInfoAllocator = std::make_shared<indexlib::index::DocInfoAllocator>();
    InitMultiComparator(attrConfigs, sortPatterns, comparator, docInfoAllocator);

    AUTIL_LOG(INFO, "init MultiComparator done");

    for (size_t i = 0; i < srcSegments.size(); i++) {
        auto [baseDocId, segment] = srcSegments[i];
        if (segment->GetSegmentInfo()->docCount == 0) {
            continue;
        }
        auto evaluator = std::make_shared<indexlib::index::MultiAttributeEvaluator>();
        auto status = InitMultiAttrEvaluator(srcSegments[i], attrConfigs, docInfoAllocator, evaluator);
        RETURN_IF_STATUS_ERROR(status, "init multiAttrEvaluator failed");
        // TODO: mv WeightedDocIterator to new dir
        SegmentMergeInfo segMergeInfo;
        segMergeInfo.baseDocId = baseDocId;
        segMergeInfo.segmentInfo = *segment->GetSegmentInfo();
        segMergeInfo.segmentId = segment->GetSegmentId();
        indexlib::index::WeightedDocIterator* docIter =
            new indexlib::index::WeightedDocIterator(segMergeInfo, evaluator, comparator, docInfoAllocator);

        if (docIter->HasNext()) {
            docIter->Next();
            docHeap.push(docIter);
        } else {
            delete docIter;
        }
    }

    uint32_t maxDocCount = srcSegments.rbegin()->first + srcSegments.rbegin()->second->GetSegmentInfo()->docCount;
    ReclaimMap::State state(targetSegmentCount, segmentSplitHandler, maxDocCount);

    AUTIL_LOG(INFO, "begin heap sort");
    std::map<segmentid_t, std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer>> mapper;
    for (size_t i = 0; i < srcSegments.size(); i++) {
        auto segment = srcSegments[i].second;
        auto segId = segment->GetSegmentId();
        auto [status, indexer] = segment->GetIndexer(indexlibv2::index::DELETION_MAP_INDEX_TYPE_STR,
                                                     indexlibv2::index::DELETION_MAP_INDEX_NAME);
        std::shared_ptr<indexlibv2::index::DeletionMapDiskIndexer> deletionmapDiskIndexer;
        if (status.IsOK()) {
            deletionmapDiskIndexer = std::dynamic_pointer_cast<indexlibv2::index::DeletionMapDiskIndexer>(indexer);
            mapper[segId] = deletionmapDiskIndexer;
        } else {
            if (!status.IsNotFound()) {
                return status;
            }
            mapper[segId] = nullptr;
        }
    }
    while (!docHeap.empty()) {
        indexlib::index::WeightedDocIterator* docIter = docHeap.top();
        docHeap.pop();
        docid_t baseId = docIter->GetBasedocId();
        docid_t localId = docIter->GetLocalDocId();
        segmentid_t segId = docIter->GetSegmentId();
        RETURN_IF_STATUS_ERROR(state.ReclaimOneDoc(baseId, segId, mapper[segId], localId), "reclaim one doc failed");

        if (docIter->HasNext()) {
            docIter->Next();
            docHeap.push(docIter);
        } else {
            delete docIter;
        }
    }
    state.FillReclaimMap(srcSegments, _deletedDocCount, _totalDocCount, _oldDocIdToNewDocId, _targetSegmentBaseDocIds);
    for (size_t i = 0; i < _targetSegmentBaseDocIds.size(); i++) {
        _targetSegmentIds.push_back(baseSegmentId++);
    }
    // calculate reverse
    CalculateReverseDocMap(srcSegments);
    AUTIL_LOG(INFO, "end sortByWeight init");
    return Status::OK();
}

void SortedReclaimMap::InitMultiComparator(
    const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
    const std::vector<config::SortPattern>& sortPatterns,
    const std::shared_ptr<indexlib::index::MultiComparator>& multiComparator,
    const std::shared_ptr<indexlib::index::DocInfoAllocator>& docInfoAllocator) const
{
    assert(attrConfigs.size() == sortPatterns.size());

    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        FieldType fieldType = attrConfigs[i]->GetFieldType();
        indexlib::index::Reference* refer = docInfoAllocator->DeclareReference(
            attrConfigs[i]->GetAttrName(), fieldType, attrConfigs[i]->GetFieldConfig()->IsEnableNullField());
        bool desc = (sortPatterns[i] != config::sp_asc);
        indexlib::index::Comparator* comparator =
            indexlib::util::ClassTypedFactory<indexlib::index::Comparator, indexlib::index::ComparatorTyped,
                                              indexlib::index::Reference*, bool>::GetInstance()
                ->Create(fieldType, refer, desc);
        multiComparator->AddComparator(std::shared_ptr<indexlib::index::Comparator>(comparator));
    }
}

Status SortedReclaimMap::InitMultiAttrEvaluator(
    const std::pair<docid_t, std::shared_ptr<framework::Segment>>& srcSegment,
    const std::vector<std::shared_ptr<indexlibv2::config::AttributeConfig>>& attrConfigs,
    const std::shared_ptr<indexlib::index::DocInfoAllocator>& docInfoAllocator,
    const std::shared_ptr<indexlib::index::MultiAttributeEvaluator>& multiEvaluatorPtr) const
{
    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        FieldType fieldType = attrConfigs[i]->GetFieldType();
        std::string attrName = attrConfigs[i]->GetAttrName();
        auto [status, diskIndexer] = srcSegment.second->GetIndexer(index::ATTRIBUTE_INDEX_TYPE_STR, attrName);
        RETURN_IF_STATUS_ERROR(status, "get attribute indexer failed");
        auto attributeDiskIndexer = std::dynamic_pointer_cast<index::AttributeDiskIndexer>(diskIndexer);
        indexlib::index::Reference* refer = docInfoAllocator->GetReference(attrName);
        indexlib::index::IEvaluator* evaluator =
            indexlib::util::ClassTypedFactory<indexlib::index::IEvaluator, indexlib::index::AttributeEvaluator,
                                              std::shared_ptr<index::AttributeDiskIndexer>,
                                              indexlib::index::Reference*>::GetInstance()
                ->Create(fieldType, attributeDiskIndexer, refer);
        multiEvaluatorPtr->AddEvaluator(std::shared_ptr<indexlib::index::IEvaluator>(evaluator));
    }

    return Status::OK();
}

}} // namespace indexlibv2::table
