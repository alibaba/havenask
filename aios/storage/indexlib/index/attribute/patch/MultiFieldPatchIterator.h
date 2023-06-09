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
#include <memory>
#include <queue>

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "indexlib/index/attribute/patch/AttributePatchIterator.h"

namespace indexlibv2::config {
class TabletSchema;
class AttributeConfig;
class PackAttributeConfig;
} // namespace indexlibv2::config

namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {
class AttributePatchIteratorComparator
{
public:
    bool operator()(AttributePatchIterator*& lft, AttributePatchIterator*& rht) const
    {
        assert(lft);
        assert(rht);
        return (*rht) < (*lft);
    }
};

class MultiFieldPatchIterator : public PatchIterator
{
public:
    MultiFieldPatchIterator(const std::shared_ptr<config::TabletSchema>& schema);
    ~MultiFieldPatchIterator();

public:
    void Init(const std::vector<std::shared_ptr<framework::Segment>>& segments, bool isSub);

    Status Next(AttributeFieldValue& value) override;
    void Reserve(AttributeFieldValue& value) override;
    bool HasNext() const override { return !_heap.empty(); }
    size_t GetPatchLoadExpandSize() const override { return _patchLoadExpandSize; };

    docid_t GetCurrentDocId() const
    {
        if (!HasNext()) {
            return INVALID_DOCID;
        }
        AttributePatchIterator* patchIter = _heap.top();
        assert(patchIter);
        return patchIter->GetCurrentDocId();
    }

    // void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;

private:
    AttributePatchIterator*
    CreateSingleFieldPatchIterator(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                   const std::shared_ptr<config::AttributeConfig>& attrConfig);

    AttributePatchIterator*
    CreatePackFieldPatchIterator(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                 const std::shared_ptr<indexlibv2::config::PackAttributeConfig>& packAttrConfig);

private:
    using AttributePatchReaderHeap = std::priority_queue<AttributePatchIterator*, std::vector<AttributePatchIterator*>,
                                                         AttributePatchIteratorComparator>;

private:
    const std::shared_ptr<config::TabletSchema> _schema;
    AttributePatchReaderHeap _heap;
    // docid_t _curDocId;
    bool _isSub;
    size_t _patchLoadExpandSize;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
