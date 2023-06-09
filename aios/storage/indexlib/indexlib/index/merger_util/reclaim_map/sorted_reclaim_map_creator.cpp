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
#include "indexlib/index/merger_util/reclaim_map/sorted_reclaim_map_creator.h"

#include <queue>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index/merger_util/reclaim_map/weighted_doc_iterator.h"
#include "indexlib/index/merger_util/truncate/attribute_evaluator.h"
#include "indexlib/index/merger_util/truncate/doc_info_allocator.h"
#include "indexlib/index/merger_util/truncate/multi_attribute_evaluator.h"
#include "indexlib/index/merger_util/truncate/multi_comparator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;
using namespace indexlib::index_base;
using indexlib::index::legacy::WeightedDocIterator;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SortedReclaimMapCreator);

SortedReclaimMapCreator::SortedReclaimMapCreator(bool hasTruncate,
                                                 const OfflineAttributeSegmentReaderContainerPtr& attrReaderContainer,
                                                 const IndexPartitionSchemaPtr& schema,
                                                 const indexlibv2::config::SortDescriptions& sortDescs)
    : ReclaimMapCreator(hasTruncate)
    , mAttrReaderContainer(attrReaderContainer)
    , mSchema(schema)
    , mSortDescs(sortDescs)
{
}

SortedReclaimMapCreator::~SortedReclaimMapCreator() {}

ReclaimMapPtr SortedReclaimMapCreator::Create(const SegmentMergeInfos& segMergeInfos,
                                              const DeletionMapReaderPtr& delMapReader,
                                              const SegmentSplitHandler& segmentSplitHandler) const
{
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    std::vector<config::AttributeConfigPtr> attrConfigs;
    vector<indexlibv2::config::SortPattern> sortPatterns;
    for (size_t i = 0; i < mSortDescs.size(); ++i) {
        const string& fieldName = mSortDescs[i].GetSortFieldName();
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(fieldName);
        if (!attrConfig) {
            INDEXLIB_FATAL_ERROR(NonExist, "Sort Field [%s] not exist in attribute!", fieldName.c_str());
        }

        if (attrConfig->IsMultiValue()) {
            INDEXLIB_FATAL_ERROR(UnImplement, "Sort Field [%s] should not be multi value!", fieldName.c_str());
        }
        attrConfigs.push_back(attrConfig);
        sortPatterns.push_back(mSortDescs[i].GetSortPattern());
    }

    int64_t before = autil::TimeUtility::currentTimeInSeconds();
    ReclaimMapPtr reclaimMap =
        SortByWeightInit(segMergeInfos, delMapReader, attrConfigs, sortPatterns, segmentSplitHandler);

    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    IE_LOG(INFO, "Init sort-by-weight reclaim map use %ld seconds", (after - before));
    return reclaimMap;
}

ReclaimMapPtr
SortedReclaimMapCreator::SortByWeightInit(const SegmentMergeInfos& segMergeInfos,
                                          const DeletionMapReaderPtr& delMapReader,
                                          const std::vector<config::AttributeConfigPtr>& attrConfigs,
                                          const std::vector<indexlibv2::config::SortPattern>& sortPatterns,
                                          const SegmentSplitHandler& segmentSplitHandler) const
{
    IE_LOG(INFO, "begin sortByWeight init");

    priority_queue<WeightedDocIterator*, vector<WeightedDocIterator*>, indexlib::index::legacy::WeightedDocIteratorComp>
        docHeap;

    legacy::MultiComparatorPtr comparator(new legacy::MultiComparator);
    legacy::DocInfoAllocatorPtr docInfoAllocator(new legacy::DocInfoAllocator);
    InitMultiComparator(attrConfigs, sortPatterns, comparator, docInfoAllocator);

    IE_LOG(INFO, "init MultiComparator done");

    for (size_t i = 0; i < segMergeInfos.size(); i++) {
        if (segMergeInfos[i].segmentInfo.docCount == 0) {
            continue;
        }

        legacy::MultiAttributeEvaluatorPtr evaluatorPtr(new legacy::MultiAttributeEvaluator);
        InitMultiAttrEvaluator(segMergeInfos[i], attrConfigs, docInfoAllocator, evaluatorPtr);

        WeightedDocIterator* docIter =
            new WeightedDocIterator(segMergeInfos[i], evaluatorPtr, comparator, docInfoAllocator);

        if (docIter->HasNext()) {
            docIter->Next();
            docHeap.push(docIter);
        } else {
            delete docIter;
        }
    }

    uint32_t maxDocCount = segMergeInfos.rbegin()->baseDocId + segMergeInfos.rbegin()->segmentInfo.docCount;
    ReclaimMapCreator::State state(segmentSplitHandler, maxDocCount);

    IE_LOG(INFO, "begin heap sort");
    while (!docHeap.empty()) {
        WeightedDocIterator* docIter = docHeap.top();
        docHeap.pop();
        docid_t baseId = docIter->GetBasedocId();
        docid_t localId = docIter->GetLocalDocId();
        segmentid_t segId = docIter->GetSegmentId();
        state.ReclaimOneDoc(segId, baseId, localId, delMapReader);

        if (docIter->HasNext()) {
            docIter->Next();
            docHeap.push(docIter);
        } else {
            delete docIter;
        }
    }
    IE_LOG(INFO, "end sortByWeight init");
    return state.ConstructReclaimMap(segMergeInfos, NeedReverseMapping(), delMapReader, true);
}

void SortedReclaimMapCreator::InitMultiComparator(const AttributeConfigVector& attrConfigs,
                                                  const std::vector<indexlibv2::config::SortPattern>& sortPatterns,
                                                  const legacy::MultiComparatorPtr& multiComparator,
                                                  const legacy::DocInfoAllocatorPtr& docInfoAllocator) const
{
    assert(attrConfigs.size() == sortPatterns.size());

    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        FieldType fieldType = attrConfigs[i]->GetFieldType();
        legacy::Reference* refer = docInfoAllocator->DeclareReference(
            attrConfigs[i]->GetAttrName(), fieldType, attrConfigs[i]->GetFieldConfig()->IsEnableNullField());
        bool desc = (sortPatterns[i] != indexlibv2::config::sp_asc);
        legacy::Comparator* comparator =
            ClassTypedFactory<legacy::Comparator, legacy::ComparatorTyped, legacy::Reference*, bool>::GetInstance()
                ->Create(fieldType, refer, desc);
        multiComparator->AddComparator(legacy::ComparatorPtr(comparator));
    }
}

void SortedReclaimMapCreator::InitMultiAttrEvaluator(const SegmentMergeInfo& segMergeInfo,
                                                     const AttributeConfigVector& attrConfigs,
                                                     const legacy::DocInfoAllocatorPtr& docInfoAllocator,
                                                     const legacy::MultiAttributeEvaluatorPtr& multiEvaluatorPtr) const
{
    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        string attrName = attrConfigs[i]->GetAttrName();
        FieldType fieldType = attrConfigs[i]->GetFieldType();
        AttributeSegmentReaderPtr attrReaderPtr;

        // TODO: legacy for unitttest
        if (segMergeInfo.TEST_inMemorySegmentReader) {
            attrReaderPtr = segMergeInfo.TEST_inMemorySegmentReader->GetAttributeSegmentReader(attrName);
        } else {
            attrReaderPtr = mAttrReaderContainer->GetAttributeSegmentReader(attrName, segMergeInfo.segmentId);
        }

        legacy::Reference* refer = docInfoAllocator->GetReference(attrName);
        legacy::Evaluator* evaluator = ClassTypedFactory<legacy::Evaluator, legacy::AttributeEvaluator,
                                                         AttributeSegmentReaderPtr, legacy::Reference*>::GetInstance()
                                           ->Create(fieldType, attrReaderPtr, refer);
        multiEvaluatorPtr->AddEvaluator(legacy::EvaluatorPtr(evaluator));
    }
}
}} // namespace indexlib::index
