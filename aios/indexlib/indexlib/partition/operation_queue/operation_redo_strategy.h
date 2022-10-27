#ifndef __INDEXLIB_OPERATION_REDO_STRATEGY_H
#define __INDEXLIB_OPERATION_REDO_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_redo_hint.h"
#include "indexlib/partition/operation_queue/remove_operation_creator.h"
#include "indexlib/util/bitmap.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
IE_NAMESPACE_BEGIN(partition);


struct OperationRedoCounter
{
    OperationRedoCounter()
        :updateOpCount(0)
        ,deleteOpCount(0)
        ,otherOpCount(0)
        ,skipRedoUpdateOpCount(0)
        ,skipRedoDeleteOpCount(0)
        ,hintOpCount(0) {}
    ~OperationRedoCounter() {}

    int64_t updateOpCount;
    int64_t deleteOpCount;
    int64_t otherOpCount;
    int64_t skipRedoUpdateOpCount;
    int64_t skipRedoDeleteOpCount;
    int64_t hintOpCount;
};

class OperationRedoStrategy
{
public:
    OperationRedoStrategy();
    ~OperationRedoStrategy();

public:
    void Init(const index_base::PartitionDataPtr& newPartitionData,
              const index_base::Version& lastVersion,
              const config::OnlineConfig& onlineConfig,
              const config::IndexPartitionSchemaPtr& schema);

    bool NeedRedo(OperationBase* operation, OperationRedoHint& redoHint);
    const OperationRedoCounter& GetCounter() const { return mRedoCounter; }
    const std::set<segmentid_t>& GetSkipDeleteSegments() const;
    
private:
    static int64_t GetMaxTimestamp(const index_base::PartitionDataPtr& partitionData);
    void InitSegmentMap(const index_base::Version& newVersion,
                        const index_base::Version& lastVersion);
    index::PrimaryKeyIndexReaderPtr LoadPrimaryKeyIndexReader(
        const index_base::PartitionDataPtr& partitionData, const config::IndexPartitionSchemaPtr& schema); 
    
private:
    bool mIsIncConsistentWithRt;
    int64_t mMaxTsInNewVersion;
    util::Bitmap mBitmap;
    std::set<segmentid_t> mSkipDelOpSegments;
    bool mHasSubSchema;
    index::PrimaryKeyIndexReaderPtr mDiffPkReader;
    index::PartitionInfoPtr mDiffPartitionInfo;
    RemoveOperationCreatorPtr mRmOpCreator; 
    IndexType mPkIndexType;
    OperationRedoCounter mRedoCounter;
    bool mEnableRedoSpeedup;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationRedoStrategy);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_OPERATION_REDO_STRATEGY_H
