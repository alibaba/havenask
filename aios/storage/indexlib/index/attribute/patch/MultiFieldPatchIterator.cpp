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
#include "indexlib/index/attribute/patch/MultiFieldPatchIterator.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/attribute/AttributeFieldValue.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/SingleFieldPatchIterator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiFieldPatchIterator);

MultiFieldPatchIterator::MultiFieldPatchIterator(const std::shared_ptr<config::TabletSchema>& schema)
    : _schema(schema)
    , _isSub(false)
    , _patchLoadExpandSize(0)
{
}

MultiFieldPatchIterator::~MultiFieldPatchIterator()
{
    while (!_heap.empty()) {
        AttributePatchIterator* iter = _heap.top();
        _heap.pop();
        delete iter;
    }
}

void MultiFieldPatchIterator::Reserve(AttributeFieldValue& value)
{
    std::vector<AttributePatchIterator*> tmp;
    tmp.reserve(_heap.size());
    while (!_heap.empty()) {
        AttributePatchIterator* iter = _heap.top();
        _heap.pop();
        tmp.push_back(iter);
        assert(iter);
        iter->Reserve(value);
    }
    for (size_t i = 0; i < tmp.size(); i++) {
        _heap.push(tmp[i]);
    }

    AUTIL_LOG(INFO, "Reserve Buffer size[%lu] for AttributeFieldValue", value.BufferLength());
}

void MultiFieldPatchIterator::Init(const std::vector<std::shared_ptr<framework::Segment>>& segments, bool isSub)
{
    _isSub = isSub;
    auto attrConfigs = _schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);

    for (const auto& indexConfig : attrConfigs) {
        const auto attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
        if (attrConfig == nullptr) {
            // pack?
            continue;
        }
        std::unique_ptr<AttributePatchIterator> attrPatchIter(CreateSingleFieldPatchIterator(segments, attrConfig));

        if (attrPatchIter) {
            _patchLoadExpandSize += attrPatchIter->GetPatchLoadExpandSize();
            _heap.push(attrPatchIter.release());
        }
    }
    // todo: add pack operation here
}

// void MultiFieldPatchIterator::CreateIndependentPatchWorkItems(vector<PatchWorkItem*>& workItems)
// {
//     vector<AttributePatchIterator*> tmp;
//     tmp.reserve(_heap.size());-
//     while (!_heap.empty()) {
//         AttributePatchIterator* iter = _heap.top();
//         _heap.pop();
//         tmp.push_back(iter);
//         assert(iter);
//         iter->CreateIndependentPatchWorkItems(workItems);
//     }

//     for (size_t i = 0; i < tmp.size(); i++) {
//         _heap.push(tmp[i]);
//     }
// }

AttributePatchIterator* MultiFieldPatchIterator::CreateSingleFieldPatchIterator(
    const std::vector<std::shared_ptr<framework::Segment>>& segments,
    const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    if (!attrConfig->IsAttributeUpdatable()) {
        return NULL;
    }
    auto singleFieldIter = std::make_unique<SingleFieldPatchIterator>(attrConfig, _isSub);
    auto status = singleFieldIter->Init(segments);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "init single field attribute patch iterator fail. field = [%s].",
                  attrConfig->GetAttrName().c_str());
        return nullptr;
    }
    if (!singleFieldIter->HasNext()) {
        return nullptr;
    }
    return singleFieldIter.release();
}

AttributePatchIterator* MultiFieldPatchIterator::CreatePackFieldPatchIterator(
    const std::vector<std::shared_ptr<framework::Segment>>& segments,
    const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& packAttrConfig)
{
    assert(false);
    return nullptr;
}

Status MultiFieldPatchIterator::Next(AttributeFieldValue& value)
{
    if (!HasNext()) {
        value.SetDocId(INVALID_DOCID);
        value.SetFieldId(INVALID_FIELDID);
        return Status::OK();
    }
    std::unique_ptr<AttributePatchIterator> patchIter(_heap.top());
    _heap.pop();
    auto status = patchIter->Next(value);
    RETURN_IF_STATUS_ERROR(status, "read next attribute patch value fail.");
    if (patchIter->HasNext()) {
        _heap.push(patchIter.release());
    } else {
        patchIter.reset();
    }
    return Status::OK();
}

} // namespace indexlibv2::index
