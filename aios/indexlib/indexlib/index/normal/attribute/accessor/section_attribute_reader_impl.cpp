#include <tr1/memory>
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"
#include "indexlib/misc/exception.h"
#include "fslib/fslib.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, SectionAttributeReaderImpl);

SectionAttributeReaderImpl::SectionAttributeReaderImpl() 
{}

SectionAttributeReaderImpl::SectionAttributeReaderImpl(
        const SectionAttributeReaderImpl& sectAttrReader)
    : SectionAttributeReader(sectAttrReader)
    , mIndexConfig(sectAttrReader.mIndexConfig)
    , mFormatter(sectAttrReader.mFormatter)
{
    if (sectAttrReader.mSectionDataReader)
    {
        SectionDataReader *clonedReader = dynamic_cast<SectionDataReader*>(
                sectAttrReader.mSectionDataReader->Clone());
        assert(clonedReader);
        mSectionDataReader.reset(clonedReader);
    }
}

SectionAttributeReaderImpl::~SectionAttributeReaderImpl() 
{
}

void SectionAttributeReaderImpl::Open(
        const config::PackageIndexConfigPtr& indexConfig,
        const index_base::PartitionDataPtr& partitionData)
{
    mIndexConfig = indexConfig;
    IE_LOG(INFO, "Start opening section attribute(%s).", 
           mIndexConfig->GetIndexName().c_str());

    SectionAttributeConfigPtr sectionAttrConf = indexConfig->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    mFormatter.reset(new SectionAttributeFormatter(sectionAttrConf));

    AttributeConfigPtr attrConf = sectionAttrConf->CreateAttributeConfig(
            indexConfig->GetIndexName());

    mSectionDataReader.reset(new SectionDataReader);
    mSectionDataReader->Open(attrConf, partitionData);

    IE_LOG(INFO, "Finish opening section attribute(%s).", 
           mIndexConfig->GetIndexName().c_str());
}

SectionAttributeReader* SectionAttributeReaderImpl::Clone() const
{
    return new SectionAttributeReaderImpl(*this);
}

IE_NAMESPACE_END(index);

