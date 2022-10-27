#include "indexlib/index/normal/inverted_index/builtin_index/pack/pack_index_merger.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger_creator.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/config/package_index_config.h"

using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackIndexMerger);

PackIndexMerger::PackIndexMerger() 
{
}

PackIndexMerger::~PackIndexMerger() 
{
}

void PackIndexMerger::Init(const IndexConfigPtr& indexConfig)
{
    IndexMerger::Init(indexConfig);

    PackageIndexConfigPtr packIndexConf = DYNAMIC_POINTER_CAST(
            PackageIndexConfig, indexConfig);
    assert(packIndexConf);
    mSectionMerger.reset();
    if (packIndexConf->GetShardingType() != IndexConfig::IST_IS_SHARDING
        && packIndexConf->HasSectionAttribute())
    {
        SectionAttributeConfigPtr sectionAttrConf = packIndexConf->GetSectionAttributeConfig();
        assert(sectionAttrConf);
        AttributeConfigPtr attrConfig = sectionAttrConf->CreateAttributeConfig(
                packIndexConf->GetIndexName());

        VarNumAttributeMergerCreator<char> mergerCreator;
        mSectionMerger.reset(mergerCreator.Create(
                        attrConfig->IsUniqEncode(), false, false));
        mSectionMerger->Init(attrConfig, mMergeHint, mTaskResources);
    }
}

void PackIndexMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir)
{
    IndexMerger::BeginMerge(segDir);
    if (mSectionMerger)
    {
        mSectionMerger->BeginMerge(segDir);
    }
}

void PackIndexMerger::SortByWeightMerge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    IndexMerger::SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
    if (mSectionMerger)
    {
        //Merge section attribute
        mSectionMerger->SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
    }
}


void PackIndexMerger::Merge(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    IndexMerger::Merge(resource, segMergeInfos, outputSegMergeInfos);
    if (mSectionMerger)
    {
        //Merge section attribute
        mSectionMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);
    }
}

OnDiskIndexIteratorCreatorPtr PackIndexMerger::CreateOnDiskIndexIteratorCreator()
{
    return OnDiskIndexIteratorCreatorPtr(
            new OnDiskPackIndexIterator::Creator(
                    GetPostingFormatOption(), mIOConfig, mIndexConfig));
}


int64_t PackIndexMerger::EstimateMemoryUse(
        const SegmentDirectoryBasePtr& segDir,
        const MergerResource& resource, 
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        bool isSortedMerge)
{
    int64_t estMemUse = IndexMerger::EstimateMemoryUse(
            segDir, resource, segMergeInfos, outputSegMergeInfos, isSortedMerge);
    if (mSectionMerger)
    {
        estMemUse = max(mSectionMerger->EstimateMemoryUse(
                        segDir, resource, segMergeInfos, outputSegMergeInfos, isSortedMerge), estMemUse);
    }
    return estMemUse;

}

IE_NAMESPACE_END(index);

