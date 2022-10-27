#ifndef __INDEXLIB_PRIMARY_KEY_LOAD_PLAN_H
#define __INDEXLIB_PRIMARY_KEY_LOAD_PLAN_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyLoadPlan
{
public:
    PrimaryKeyLoadPlan();
    ~PrimaryKeyLoadPlan();

public:
    void Init(docid_t baseDocid, const config::PrimaryKeyIndexConfigPtr& indexConfig);

public:
    void AddSegmentData(const index_base::SegmentData& segmentData, 
                        size_t deleteDocCount)
    { 
        mSegmentDatas.push_back(segmentData); 
        mDocCount += segmentData.GetSegmentInfo().docCount;
        mDeleteDocCount += deleteDocCount;
    }

    bool CanDirectLoad() const;

    docid_t GetBaseDocId() const
    { return mBaseDocid; }
    size_t GetDocCount() const
    { return mDocCount; }
    size_t GetDeletedDocCount() const
    { return mDeleteDocCount; }
    std::string GetTargetFileName() const;
    index_base::SegmentDataVector GetLoadSegmentDatas() const
    { return mSegmentDatas; }
    file_system::DirectoryPtr GetTargetFileDirectory() const;

    size_t GetSegmentCount() const
    { return mSegmentDatas.size(); }
    
private:
    docid_t mBaseDocid;
    size_t mDocCount;
    size_t mDeleteDocCount;
    config::PrimaryKeyIndexConfigPtr mIndexConfig;
    index_base::SegmentDataVector mSegmentDatas;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(PrimaryKeyLoadPlan);
typedef std::vector<PrimaryKeyLoadPlanPtr> PrimaryKeyLoadPlanVector;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_LOAD_PLAN_H
