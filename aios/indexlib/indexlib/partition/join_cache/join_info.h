#ifndef __INDEXLIB_JOININFO_H
#define __INDEXLIB_JOININFO_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "indexlib/partition/join_cache/join_docid_reader_base.h"
#include "indexlib/partition/join_cache/join_docid_reader_creator.h"
#include "indexlib/partition/join_cache/join_cache_initializer_creator.h"

IE_NAMESPACE_BEGIN(partition);
class JoinInfo {
public:
    JoinInfo() {}
    
public:
    void Init(bool isSubJoin, const JoinField& joinField,
              uint64_t auxPartitionIdentifier,
              const IndexPartitionReaderPtr& mainIndexPartReader,
              const IndexPartitionReaderPtr& joinIndexPartReader)
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
    bool IsStrongJoin() const { return mJoinField.isStrongJoin; }
    bool UseJoinCache() const { return mJoinField.genJoinCache; }
    const std::string& GetJoinFieldName() const { return mJoinField.fieldName; }

    void SetMainPartitionReader(const IndexPartitionReaderPtr& mainIndexPartReader)
    { mMainPartReaderPtr = mainIndexPartReader; }
    void SetJoinPartitionReader(const IndexPartitionReaderPtr& joinIndexPartReader)
    { mJoinPartReaderPtr = joinIndexPartReader; }
    IndexPartitionReaderPtr GetMainPartitionReader() const
    { return mMainPartReaderPtr; }
    IndexPartitionReaderPtr GetJoinPartitionReader() const
    { return mJoinPartReaderPtr; }
    JoinDocidReaderBase* CreateJoinDocidReader(autil::mem_pool::Pool* pool) const
    {
        return JoinDocidReaderCreator::Create(mMainPartReaderPtr, mJoinPartReaderPtr,
                mJoinField, mAuxPartitionIdentifier, mIsSubJoin, pool);
    }

    // TODO: provide JoinDocIdAttrReader directly
    std::string GetJoinVirtualAttrName() const
    {
        if (mJoinDocIdAttrName.empty())
        {
            mJoinDocIdAttrName =
                JoinCacheInitializerCreator::GenerateVirtualAttributeName(
                        IsSubJoin() ? BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX
                        : BUILD_IN_JOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX,
                        mAuxPartitionIdentifier, mJoinField.fieldName,
                        mJoinPartReaderPtr->GetIncVersionId());
        }
        return mJoinDocIdAttrName;
    }
    
        
private:
    bool mIsSubJoin;
    mutable std::string mJoinDocIdAttrName;
    JoinField mJoinField;
    IndexPartitionReaderPtr mMainPartReaderPtr;
    IndexPartitionReaderPtr mJoinPartReaderPtr;
    uint64_t mAuxPartitionIdentifier;
};

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOININFO_H
