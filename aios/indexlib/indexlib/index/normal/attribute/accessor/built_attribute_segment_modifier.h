#ifndef __INDEXLIB_BUILT_ATTRIBUTE_SEGMENT_MODIFIER_H
#define __INDEXLIB_BUILT_ATTRIBUTE_SEGMENT_MODIFIER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_modifier.h"
#include "indexlib/index/dump_item_typed.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

class BuiltAttributeSegmentModifier : public AttributeSegmentModifier
{
public:
    BuiltAttributeSegmentModifier(segmentid_t segId,
                                  const config::AttributeSchemaPtr& attrSchema,
                                  util::BuildResourceMetrics* buildResourceMetrics);

    virtual ~BuiltAttributeSegmentModifier();

public:
    void Update(docid_t docId, attrid_t attrId, 
                const autil::ConstString& attrValue) override;
    
    void Dump(const file_system::DirectoryPtr& dir,
              segmentid_t srcSegment) override;

    void CreateDumpItems(const file_system::DirectoryPtr& directory,
                         segmentid_t srcSegmentId,
                         std::vector<common::DumpItem*>& dumpItems);

private:
    AttributeUpdaterPtr& GetUpdater(uint32_t idx);
    //virtual for test
    virtual AttributeUpdaterPtr CreateUpdater(uint32_t idx);
    
private:
    config::AttributeSchemaPtr mAttrSchema;
    segmentid_t mSegmentId;
    std::vector<AttributeUpdaterPtr> mUpdaters;
    util::BuildResourceMetrics* mBuildResourceMetrics;

private:
    friend class BuiltAttributeSegmentModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltAttributeSegmentModifier);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILT_ATTRIBUTE_SEGMENT_MODIFIER_H
