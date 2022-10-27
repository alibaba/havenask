#include "indexlib/table/table_merge_plan_meta.h"

using namespace std;

IE_NAMESPACE_BEGIN(table);
IE_LOG_SETUP(table, TableMergePlanMeta);

TableMergePlanMeta::TableMergePlanMeta()
    : locator()
    , timestamp(INVALID_TIMESTAMP)
    , maxTTL(0)
    , segmentDescription()
{
}

TableMergePlanMeta::~TableMergePlanMeta() 
{
}

void TableMergePlanMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        string locatorString = autil::StringUtil::strToHexStr(locator.ToString());
        json.Jsonize("target_segment_locator", locatorString);
    } else { 
        string locatorString;
        json.Jsonize("target_segment_locator", locatorString);
        locatorString = autil::StringUtil::hexStrToStr(locatorString);
        locator.SetLocator(locatorString);
    }
    json.Jsonize("target_segment_timestamp", timestamp);
    json.Jsonize("target_segment_maxTTL", maxTTL);
    json.Jsonize("target_segment_id", targetSegmentId);
    json.Jsonize("target_segment_description", segmentDescription); 
}


IE_NAMESPACE_END(table);

