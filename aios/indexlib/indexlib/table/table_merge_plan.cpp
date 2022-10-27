#include "indexlib/table/table_merge_plan.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableMergePlan);

TableMergePlan::TableMergePlan() 
{
}

TableMergePlan::~TableMergePlan() 
{
}

void TableMergePlan::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        vector<segmentid_t> segIds;
        vector<bool> reserveFlags;
        segIds.reserve(mInPlanSegAttrs.size());
        reserveFlags.reserve(mInPlanSegAttrs.size());        
        for (const auto& segAttr: mInPlanSegAttrs)
        {
            segIds.push_back(segAttr.first);
            reserveFlags.push_back(segAttr.second);
        }
        json.Jsonize("in_plan_segments", segIds);
        json.Jsonize("reserve_flags", reserveFlags); 
    }
    else
    {
        vector<segmentid_t> segIds;
        vector<bool> reserveFlags;        
        json.Jsonize("in_plan_segments", segIds);
        json.Jsonize("reserve_flags", reserveFlags);
        for (size_t i = 0; i < segIds.size(); i++)
        {
            mInPlanSegAttrs.insert(make_pair(segIds[i], reserveFlags[i])); 
        }
    }
}

void TableMergePlan::AddSegment(segmentid_t segmentId, bool reservedInNewVersion)
{
    mInPlanSegAttrs.insert(make_pair(segmentId, reservedInNewVersion));
}

const TableMergePlan::InPlanSegmentAttributes& TableMergePlan::GetInPlanSegments() const
{
    return mInPlanSegAttrs;
}

IE_NAMESPACE_END(table);

