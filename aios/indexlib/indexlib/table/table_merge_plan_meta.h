#ifndef __INDEXLIB_TABLE_MERGE_PLAN_META_H
#define __INDEXLIB_TABLE_MERGE_PLAN_META_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/table/table_common.h"
#include "indexlib/document/locator.h"
#include <autil/legacy/jsonizable.h>

IE_NAMESPACE_BEGIN(table);

class TableMergePlanMeta : public autil::legacy::Jsonizable
{
public:
    TableMergePlanMeta();
    ~TableMergePlanMeta();
public:    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;    
public:
    document::Locator locator;
    int64_t timestamp;
    uint32_t maxTTL;
    segmentid_t targetSegmentId;
    MergeSegmentDescription segmentDescription;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergePlanMeta);

IE_NAMESPACE_END(table);

#endif //__INDEXLIB_TABLE_MERGE_PLAN_META_H
