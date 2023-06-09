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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

////////////////////////////////////////////////////////////////////////////////////////////
// Read from join doc id attribute and temporary main to sub doc id map. The main to sub doc id map is used for batch
// build and keeps state for docs that are in the batch but not yet built.

// CompositeJoinDocIdAttributeReader是JoinDocIdAttributeReader在batch build场景的一个子类。用于处理batch
// build在batch构建过程未结束时，batch内doc没有及时更新到索引上，也没有更新比如partition
// info等信息，导致部分数据结构(这里是JoinDocIdAttribute)无法通过读索引直接找到。
// 这里只能将信息额外记录到CompositeJoinDocIdAttributeReader的内存态辅助结构中。
// 当CompositeJoinDocIdAttributeReader对应的batch结束时，临时的内存态辅助结构会实际写到索引中，之后索引状态会和流式构建一致。

class CompositeJoinDocIdReader : public JoinDocidAttributeReader
{
public:
    CompositeJoinDocIdReader() {}
    ~CompositeJoinDocIdReader() {}

public:
    void Init(const index::JoinDocidAttributeReaderPtr& joinDocIdAttributeReader)
    {
        return Reinit(joinDocIdAttributeReader);
    }
    void Reinit(const index::JoinDocidAttributeReaderPtr& joinDocIdAttributeReader);
    docid_t GetJoinDocId(docid_t docId) const override;
    void Insert(docid_t mainDocId, docid_t subDocIdEnd);

private:
    JoinDocidAttributeReaderPtr _joinDocIdReader;
    // main doc id->end sub doc id
    std::map<docid_t, docid_t> _mainToSubDocIdMap;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::index
