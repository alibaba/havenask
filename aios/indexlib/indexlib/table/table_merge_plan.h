#ifndef __INDEXLIB_TABLE_MERGE_PLAN_H
#define __INDEXLIB_TABLE_MERGE_PLAN_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(table);

class TableMergePlan : public autil::legacy::Jsonizable
{
public:
    TableMergePlan();
    virtual ~TableMergePlan();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override; 
    void AddSegment(segmentid_t segmentId, bool reserveInNewVersion = false);
    const std::map<segmentid_t, bool>& GetInPlanSegments() const;
    
public:
    typedef std::map<segmentid_t, bool> InPlanSegmentAttributes;
    typedef InPlanSegmentAttributes::value_type InPlanSegmentAttribute;
    
private:
    InPlanSegmentAttributes mInPlanSegAttrs;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergePlan);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_MERGE_PLAN_H
