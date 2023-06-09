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
#include "indexlib/partition/patch_loader.h"

#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_field_patch_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_patch_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_patch_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_patch_work_item.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"

using namespace std;
using namespace autil;

using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PatchLoader);

int64_t PatchLoader::PATCH_WORK_ITEM_COST_SAMPLE_COUNT = 2000;

PatchLoader::PatchLoader(const IndexPartitionSchemaPtr& schema, const OnlineConfig& onlineConfig)
    : _schema(schema)
    , _onlineConfig(onlineConfig)
{
}

PatchLoader::~PatchLoader()
{
    for (size_t i = 0; i < _patchWorkItems.size(); ++i) {
        if (_patchWorkItems[i]) {
            delete _patchWorkItems[i];
            _patchWorkItems[i] = NULL;
        }
    }
}

void PatchLoader::Init(const PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                       const Version& lastLoadVersion, segmentid_t startLoadSegment, bool loadPatchForOpen)
{
    _partitionData = partitionData;
    int64_t timeBegin = TimeUtility::currentTime();
    _attrPatchIter = PatchIteratorCreator::Create(_schema, partitionData, ignorePatchToOldIncSegment, lastLoadVersion,
                                                  startLoadSegment);
    _indexPatchIter = IndexPatchIteratorCreator::Create(_schema, partitionData, ignorePatchToOldIncSegment,
                                                        lastLoadVersion, startLoadSegment);
    if (loadPatchForOpen && _onlineConfig.loadPatchThreadNum > 1) {
        _attrPatchIter->CreateIndependentPatchWorkItems(_patchWorkItems);
        _indexPatchIter->CreateIndependentPatchWorkItems(_patchWorkItems);
    }
    int64_t timeAfterCreate = TimeUtility::currentTime();
    IE_LOG(INFO, "load patch, create iterator use [%ld] ms", (timeAfterCreate - timeBegin) / 1000);
}

size_t PatchLoader::CalculatePatchLoadExpandSize()
{
    return _attrPatchIter->GetPatchLoadExpandSize() + _indexPatchIter->GetPatchLoadExpandSize();
};

std::pair<size_t, size_t> PatchLoader::SingleThreadLoadPatch(const PartitionModifierPtr& modifier)
{
    IE_LOG(INFO, "single thread load attribute patch start");
    size_t attrCount = 0;
    AttrFieldValue value;
    _attrPatchIter->Reserve(value);
    while (_attrPatchIter->HasNext()) {
        _attrPatchIter->Next(value);
        modifier->UpdateField(value);

        ++attrCount;
    }
    IE_LOG(INFO, "single thread load attribute patch end");
    IE_LOG(INFO, "single thread load index patch start");
    size_t indexCount = 0;
    while (true) {
        std::unique_ptr<index::IndexUpdateTermIterator> termIter = _indexPatchIter->Next();
        if (termIter == nullptr) {
            break;
        }
        modifier->UpdateIndex(termIter.get());
        indexCount += termIter->GetProcessedDocs();
    }
    IE_LOG(INFO, "single thread load index patch end");
    return {attrCount, indexCount};
}

bool PatchLoader::InitPatchWorkItems(const PartitionModifierPtr& modifier, vector<PatchWorkItem*>* workItems)
{
    const IndexPartitionSchemaPtr& subSchema = _schema->GetSubIndexPartitionSchema();

    PartitionModifierPtr mainModifier = modifier;
    PartitionModifierPtr subModifier;

    if (subSchema) {
        SubDocModifierPtr subDocModifier = DYNAMIC_POINTER_CAST(SubDocModifier, modifier);
        if (!subDocModifier) {
            IE_LOG(ERROR, "modifier cannot be cast to SubDocModifier");
            return false;
        }
        mainModifier = subDocModifier->GetMainModifier();
        subModifier = subDocModifier->GetSubModifier();
    }
    for (auto workItem : *workItems) {
        auto partModifier = workItem->IsSub() ? subModifier : mainModifier;

        auto inplaceModifier = DYNAMIC_POINTER_CAST(partition::InplaceModifier, partModifier);
        if (!inplaceModifier) {
            IE_LOG(ERROR, "modifier cannot be cast to InplaceModifier when initializing [%s]",
                   workItem->GetIdentifier().c_str());
            return false;
        }
        auto deletionMapReader = inplaceModifier->GetDeletionMapReader();

        if (workItem->GetItemType() == PatchWorkItem::PWIT_INDEX) {
            auto indexPatchWorkItem = static_cast<IndexPatchWorkItem*>(workItem);
            index::InplaceIndexModifier* indexModifier = inplaceModifier->GetIndexModifier();
            if (!indexPatchWorkItem->Init(deletionMapReader, indexModifier)) {
                IE_LOG(ERROR, "Init IndexPatchWorkItem[%s] failed", indexPatchWorkItem->GetIdentifier().c_str());
                return false;
            }
        } else {
            auto attrPatchWorkItem = static_cast<AttributePatchWorkItem*>(workItem);
            auto attrModifier = inplaceModifier->GetAttributeModifier();
            if (!attrPatchWorkItem->Init(deletionMapReader, attrModifier)) {
                IE_LOG(ERROR, "Init AttrPatchWorkItem[%s] failed", attrPatchWorkItem->GetIdentifier().c_str());
                return false;
            }
        }
    }
    return true;
}

std::pair<size_t, size_t> PatchLoader::EstimatePatchWorkItemCost(vector<index::PatchWorkItem*>* patchWorkItems,
                                                                 const ThreadPoolPtr& threadPool)
{
    auto CompareByTypeAndCount = [](const PatchWorkItem* lhs, const PatchWorkItem* rhs) {
        if (lhs->GetItemType() == rhs->GetItemType()) {
            return lhs->GetPatchItemCount() > rhs->GetPatchItemCount();
        }
        return lhs->GetItemType() > rhs->GetItemType();
    };
    sort(patchWorkItems->begin(), patchWorkItems->end(), CompareByTypeAndCount);
    for (auto& workItem : *patchWorkItems) {
        workItem->SetProcessLimit(PATCH_WORK_ITEM_COST_SAMPLE_COUNT);
        if (ThreadPool::ERROR_NONE != threadPool->pushWorkItem(workItem)) {
            ThreadPool::dropItemIgnoreException(workItem);
        }
    }
    threadPool->waitFinish();
    size_t attrCount = 0;
    size_t indexCount = 0;
    for (auto& workItem : *patchWorkItems) {
        size_t lastProcessCount = workItem->GetLastProcessCount();
        size_t totalPatchCount = workItem->GetPatchItemCount();
        int64_t lastProcessTimeInMicroSeconds = workItem->GetLastProcessTime();
        size_t estimateTotalProcessTime =
            (lastProcessCount == 0) ? 0 : lastProcessTimeInMicroSeconds * (totalPatchCount / lastProcessCount);
        assert(estimateTotalProcessTime - lastProcessTimeInMicroSeconds >= 0);
        workItem->SetCost(estimateTotalProcessTime - lastProcessTimeInMicroSeconds);
        if (workItem->GetItemType() == PatchWorkItem::PatchWorkItemType::PWIT_INDEX) {
            indexCount += lastProcessCount;
        } else {
            attrCount += lastProcessCount;
        }
    }
    return {attrCount, indexCount};
}

void PatchLoader::SortPatchWorkItemsByCost(vector<index::PatchWorkItem*>* patchWorkItems)
{
    auto CompareByCost = [](const PatchWorkItem* lhs, const PatchWorkItem* rhs) {
        return lhs->GetCost() > rhs->GetCost();
    };
    sort(patchWorkItems->begin(), patchWorkItems->end(), CompareByCost);
}

std::pair<size_t, size_t> PatchLoader::MultiThreadLoadPatch(const PartitionModifierPtr& modifier)
{
    if (!InitPatchWorkItems(modifier, &_patchWorkItems)) {
        INDEXLIB_FATAL_ERROR(Runtime, "init patch work items failed");
    }
    ThreadPoolPtr threadPool(new ThreadPool(_onlineConfig.loadPatchThreadNum, _patchWorkItems.size(), true));
    threadPool->start("indexLoadPatch");
    size_t attrCount = 0;
    size_t indexCount = 0;
    // Estimate cost by first applying some of the patches, count the number of patch items and time, then estimate time
    // cost per item.
    auto countPair = EstimatePatchWorkItemCost(&_patchWorkItems, threadPool);
    attrCount += countPair.first;
    indexCount += countPair.second;
    SortPatchWorkItemsByCost(&_patchWorkItems);

    // Continue applying patches.
    for (size_t i = 0; i < _patchWorkItems.size(); ++i) {
        _patchWorkItems[i]->SetProcessLimit(numeric_limits<size_t>::max());
        if (ThreadPool::ERROR_NONE != threadPool->pushWorkItem(_patchWorkItems[i])) {
            ThreadPool::dropItemIgnoreException(_patchWorkItems[i]);
        }
    }
    threadPool->waitFinish();
    threadPool->stop();

    for (size_t i = 0; i < _patchWorkItems.size(); ++i) {
        if (_patchWorkItems[i]->GetItemType() == PatchWorkItem::PatchWorkItemType::PWIT_INDEX) {
            indexCount += _patchWorkItems[i]->GetLastProcessCount();
        } else {
            attrCount += _patchWorkItems[i]->GetLastProcessCount();
        }
    }
    return {attrCount, indexCount};
}

void PatchLoader::Load(const Version& patchLoadedVersion, const PartitionModifierPtr& modifier)
{
    assert(_partitionData);
    if (!NeedLoadPatch(_partitionData->GetVersion(), patchLoadedVersion)) {
        return;
    }

    autil::ScopedTime2 timer;
    std::pair<size_t, size_t> count {0, 0};
    uint32_t threadCount = _onlineConfig.loadPatchThreadNum;
    if (_patchWorkItems.size() == 0 || threadCount == 1) {
        count = SingleThreadLoadPatch(modifier);
    } else {
        IE_LOG(INFO, "enable multi thread load patch, threadNum = [%u]", threadCount);
        count = MultiThreadLoadPatch(modifier);
    }
    IE_LOG(INFO, "load finish, total updated attr field [%zu], index [%zu], used[%.3f]s", count.first, count.second,
           timer.done_sec());
}

bool PatchLoader::NeedLoadPatch(const Version& currentVersion, const Version& loadedVersion) const
{
    if (!(currentVersion > loadedVersion)) {
        return false;
    }

    if (HasUpdatableAttribute(_schema) or HasUpdatableIndex(_schema)) {
        return true;
    }

    IndexPartitionSchemaPtr subSchema = _schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        return HasUpdatableAttribute(subSchema) or HasUpdatableIndex(subSchema);
    }
    return false;
}

// only for test
void PatchLoader::LoadAttributePatch(AttributeReader& attrReader, const AttributeConfigPtr& attrConfig,
                                     const PartitionDataPtr& partitionData)
{
    assert(attrConfig);
    assert(partitionData);

    SingleFieldPatchIterator patchIter(attrConfig, false);
    patchIter.Init(partitionData, false, INVALID_SEGMENTID);

    AttrFieldValue value;
    patchIter.Reserve(value);
    while (patchIter.HasNext()) {
        patchIter.Next(value);
        attrReader.UpdateField(value.GetDocId(), value.Data(), value.GetDataSize());
    }
}

bool PatchLoader::HasUpdatableAttribute(const config::IndexPartitionSchemaPtr& schema)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    if (!indexSchema->HasPrimaryKeyIndex()) {
        return false;
    }

    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (!attrSchema) {
        return false;
    }

    bool hasUpdateAttribute = false;
    auto attrConfigs = attrSchema->CreateIterator();
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->IsAttributeUpdatable()) {
            hasUpdateAttribute = true;
            break;
        }
    }
    return hasUpdateAttribute;
}

bool PatchLoader::HasUpdatableIndex(const config::IndexPartitionSchemaPtr& schema)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    if (!indexSchema->HasPrimaryKeyIndex()) {
        return false;
    }
    return indexSchema->AnyIndexUpdatable();
}
}} // namespace indexlib::partition
