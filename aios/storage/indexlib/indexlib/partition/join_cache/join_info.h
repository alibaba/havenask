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

#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/join_cache/join_field.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);

namespace indexlib { namespace partition {
class JoinInfo
{
public:
    JoinInfo() {}

public:
    void Init(bool isSubJoin, const JoinField& joinField, uint64_t auxPartitionIdentifier,
              const IndexPartitionReaderPtr& mainIndexPartReader, const IndexPartitionReaderPtr& joinIndexPartReader)
    {
        mIsSubJoin = isSubJoin;
        mJoinField = joinField;
        mMainPartReaderPtr = mainIndexPartReader;
        mJoinPartReaderPtr = joinIndexPartReader;
        mAuxPartitionIdentifier = auxPartitionIdentifier;
    }

    void SetIsSubJoin(bool isSubJoin) { mIsSubJoin = isSubJoin; }
    bool IsSubJoin() const { return mIsSubJoin; }

    void SetJoinField(const JoinField& joinField) { mJoinField = joinField; }
    const JoinField& GetJoinField() const { return mJoinField; }

    bool IsStrongJoin() const { return mJoinField.isStrongJoin; }
    bool UseJoinCache() const { return mJoinField.genJoinCache; }
    const std::string& GetJoinFieldName() const { return mJoinField.fieldName; }

    void SetMainPartitionReader(const IndexPartitionReaderPtr& mainIndexPartReader)
    {
        mMainPartReaderPtr = mainIndexPartReader;
    }
    void SetJoinPartitionReader(const IndexPartitionReaderPtr& joinIndexPartReader)
    {
        mJoinPartReaderPtr = joinIndexPartReader;
    }
    IndexPartitionReaderPtr GetMainPartitionReader() const { return mMainPartReaderPtr; }
    IndexPartitionReaderPtr GetJoinPartitionReader() const { return mJoinPartReaderPtr; }

    uint64_t GetAuxPartitionIdentifier() const { return mAuxPartitionIdentifier; }

    // TODO: provide JoinDocIdAttrReader directly
    std::string GetJoinVirtualAttrName() const;

private:
    bool mIsSubJoin;
    mutable std::string mJoinDocIdAttrName;
    JoinField mJoinField;
    IndexPartitionReaderPtr mMainPartReaderPtr;
    IndexPartitionReaderPtr mJoinPartReaderPtr;
    uint64_t mAuxPartitionIdentifier;
};
}} // namespace indexlib::partition
