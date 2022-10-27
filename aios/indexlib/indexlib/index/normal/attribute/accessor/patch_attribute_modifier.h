#ifndef __INDEXLIB_PATCH_ATTRIBUTE_MODIFIER_H
#define __INDEXLIB_PATCH_ATTRIBUTE_MODIFIER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(index, BuiltAttributeSegmentModifier);

IE_NAMESPACE_BEGIN(index);

class PatchAttributeModifier : public AttributeModifier
{
private:
    struct SegmentBaseDocIdInfo
    {
        SegmentBaseDocIdInfo()
            : segId(INVALID_SEGMENTID)
            , baseDocId(INVALID_DOCID)
        {
        }

        SegmentBaseDocIdInfo(segmentid_t segmentId, docid_t docId)
            : segId(segmentId)
            , baseDocId(docId)
        {
        }

        segmentid_t segId;
        docid_t baseDocId;
    };

    typedef std::vector<SegmentBaseDocIdInfo> SegBaseDocIdInfoVect;
    typedef std::vector<util::AccumulativeCounterPtr> AttrCounterVector;    

public:
    PatchAttributeModifier(const config::IndexPartitionSchemaPtr& schema,
                           util::BuildResourceMetrics* buildResourceMetrics,
                           bool isOffline = false);
    ~PatchAttributeModifier();

public:
    void Init(const index_base::PartitionDataPtr& partitionData);
    bool Update(
            docid_t docId, const document::AttributeDocumentPtr& attrDoc) override;
    bool UpdateField(docid_t docId, fieldid_t fieldId, 
                                   const autil::ConstString& value) override;

    void Dump(const file_system::DirectoryPtr& dir, 
              segmentid_t srcSegment = INVALID_SEGMENTID,
              uint32_t dumpThreadNum = 1);

    bool IsDirty() const { return !mSegId2BuiltModifierMap.empty(); }

    void GetPatchedSegmentIds(
            std::vector<segmentid_t> &patchSegIds) const;

private:
    BuiltAttributeSegmentModifierPtr GetSegmentModifier(
            docid_t globalId, docid_t &localId);

    const SegmentBaseDocIdInfo& GetSegmentBaseInfo(
            docid_t globalId) const;

    // virtual for test
    virtual BuiltAttributeSegmentModifierPtr CreateBuiltSegmentModifier(
            segmentid_t segmentId);

    void InitCounters(const util::CounterMapPtr& counterMap);

private:
    typedef std::map<segmentid_t, BuiltAttributeSegmentModifierPtr> BuiltSegmentModifierMap;

private:
    BuiltSegmentModifierMap mSegId2BuiltModifierMap;
    SegBaseDocIdInfoVect mDumpedSegmentBaseIdVect;
    AttrCounterVector mAttrIdToCounters;    
    bool mIsOffline;
    bool mEnableCounters;

private:
    friend class PatchAttributeModifierTest;
    IE_LOG_DECLARE();
};

inline const PatchAttributeModifier::SegmentBaseDocIdInfo& 
PatchAttributeModifier::GetSegmentBaseInfo(docid_t globalId) const
{
    size_t i = 1;
    for (; i < mDumpedSegmentBaseIdVect.size(); ++i)
    {
        if (globalId < mDumpedSegmentBaseIdVect[i].baseDocId)
        {
            break;
        }
    }

    i--;
    return mDumpedSegmentBaseIdVect[i];
}

inline BuiltAttributeSegmentModifierPtr
PatchAttributeModifier::GetSegmentModifier(docid_t globalId, docid_t &localId)
{
    const SegmentBaseDocIdInfo &segBaseIdInfo = GetSegmentBaseInfo(globalId);
    localId = globalId - segBaseIdInfo.baseDocId;
    segmentid_t segId = segBaseIdInfo.segId;

    BuiltSegmentModifierMap::iterator iter = 
        mSegId2BuiltModifierMap.find(segId);
    if (iter == mSegId2BuiltModifierMap.end())
    {
        mSegId2BuiltModifierMap[segId] = CreateBuiltSegmentModifier(segId);
    }
    return mSegId2BuiltModifierMap[segId];
}

DEFINE_SHARED_PTR(PatchAttributeModifier);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PATCH_ATTRIBUTE_MODIFIER_H
