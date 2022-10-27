#include "indexlib/index/normal/attribute/accessor/built_attribute_segment_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater_factory.h"

using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BuiltAttributeSegmentModifier);

BuiltAttributeSegmentModifier::BuiltAttributeSegmentModifier(
        segmentid_t segId,
        const AttributeSchemaPtr& attrSchema,
        util::BuildResourceMetrics* buildResourceMetrics)
    : mAttrSchema(attrSchema)
    , mSegmentId(segId)
    , mBuildResourceMetrics(buildResourceMetrics)
{}

BuiltAttributeSegmentModifier::~BuiltAttributeSegmentModifier() 
{}

void BuiltAttributeSegmentModifier::Update(docid_t docId, attrid_t attrId,
        const autil::ConstString& attrValue)
{
    AttributeUpdaterPtr &updater = GetUpdater((uint32_t)attrId);
    assert(updater);
    updater->Update(docId, attrValue);
}

AttributeUpdaterPtr BuiltAttributeSegmentModifier::CreateUpdater(uint32_t idx)
{
    attrid_t attrId = (attrid_t)idx;
    IE_LOG(DEBUG, "Create Attribute Updater %d in segment %d", attrId, mSegmentId);

    AttributeUpdaterFactory* factory = AttributeUpdaterFactory::GetInstance();
    AttributeConfigPtr attrConfig = mAttrSchema->GetAttributeConfig(attrId);
    AttributeUpdater* updater =
        factory->CreateAttributeUpdater(
                mBuildResourceMetrics, mSegmentId, attrConfig);
    assert(updater != NULL);
    return AttributeUpdaterPtr(updater);
}

void BuiltAttributeSegmentModifier::Dump(
        const DirectoryPtr& dir, segmentid_t srcSegment)
{
    for (size_t i = 0; i < mUpdaters.size(); ++i)
    {
        if (mUpdaters[i])
        {
            mUpdaters[i]->Dump(dir, srcSegment);
        }
    }
}

AttributeUpdaterPtr& BuiltAttributeSegmentModifier::GetUpdater(uint32_t idx)
{
    if (idx >= (uint32_t)mUpdaters.size())
    {
        mUpdaters.resize(idx + 1);
    }

    if (!mUpdaters[idx])
    {
        mUpdaters[idx] = CreateUpdater(idx);
    }
    return mUpdaters[idx];
}

void BuiltAttributeSegmentModifier::CreateDumpItems(
        const DirectoryPtr& directory, segmentid_t srcSegmentId,
        vector<common::DumpItem*>& dumpItems)
{
    for (size_t i = 0; i < mUpdaters.size(); ++i)
    {
        if (mUpdaters[i])
        {
            dumpItems.push_back(new AttributeUpdaterDumpItem(
                            directory, mUpdaters[i], srcSegmentId));
        }
    }
}


IE_NAMESPACE_END(index);

