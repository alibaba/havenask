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

#include "indexlib/common/field_format/attribute/attribute_value_initializer.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);

namespace indexlib { namespace partition {

class JoinCacheInitializer : public common::AttributeValueInitializer
{
public:
    JoinCacheInitializer();
    ~JoinCacheInitializer();

public:
    void Init(const index::AttributeReaderPtr& attrReader, const IndexPartitionReaderPtr auxReader);

    /* override*/ bool GetInitValue(docid_t docId, char* buffer, size_t bufLen) const;

    /* override*/ bool GetInitValue(docid_t docId, autil::StringView& value, autil::mem_pool::Pool* memPool) const;

private:
    docid_t GetJoinDocId(docid_t docId) const;

private:
    index::AttributeReaderPtr mJoinFieldAttrReader;
    IndexPartitionReaderPtr mAuxPartitionReader;
    index::PrimaryKeyIndexReaderPtr mPkIndexReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinCacheInitializer);
}} // namespace indexlib::partition
