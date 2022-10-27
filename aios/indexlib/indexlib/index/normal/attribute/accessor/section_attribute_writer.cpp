#include "indexlib/index/normal/attribute/accessor/section_attribute_writer.h"
#include "fslib/fslib.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/document/index_document/normal_document/index_document.h"
#include "indexlib/common/field_format/attribute/string_attribute_convertor.h"

using namespace std;
using namespace std::tr1;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);

IE_LOG_SETUP(index, SectionAttributeWriter);

SectionAttributeWriter::SectionAttributeWriter(const PackageIndexConfigPtr& config)
    : mConfig(config)
    , mIndexId(INVALID_INDEXID)
{
    assert(mConfig);
    mIndexId = mConfig->GetIndexId();
}

void SectionAttributeWriter::Init(BuildResourceMetrics* buildResourceMetrics)
{
    SectionAttributeConfigPtr sectionAttrConf = mConfig->GetSectionAttributeConfig();
    assert(sectionAttrConf);
    AttributeConfigPtr attrConfig = sectionAttrConf->CreateAttributeConfig(
            mConfig->GetIndexName());

    AttributeConvertorPtr attrConvertor(
	new StringAttributeConvertor(
	    attrConfig->IsUniqEncode(), attrConfig->GetAttrName()));

    mAttributeWriter.reset(new StringAttributeWriter(attrConfig));
    mAttributeWriter->SetAttrConvertor(attrConvertor);
    mAttributeWriter->Init(FSWriterParamDeciderPtr(), buildResourceMetrics);
}

bool SectionAttributeWriter::EndDocument(const IndexDocument& indexDocument)
{
    docid_t docId = indexDocument.GetDocId();
    if (docId < 0)
    {
        IE_LOG(WARN, "Invalid doc id(%d).", docId);        
        return false;
    }

    const ConstString& sectionAttrStr = indexDocument.GetSectionAttribute(mIndexId);
    assert(sectionAttrStr != ConstString::EMPTY_STRING);
    assert(mAttributeWriter);
    mAttributeWriter->AddField(docId, sectionAttrStr);
    return true;
}

void SectionAttributeWriter::Dump(const file_system::DirectoryPtr &dir,
                                  autil::mem_pool::PoolBase* dumpPool)
{
    assert(mAttributeWriter);
    mAttributeWriter->Dump(dir, dumpPool);
}

AttributeSegmentReaderPtr SectionAttributeWriter::CreateInMemReader() const
{
    return mAttributeWriter->CreateInMemReader();
}

void SectionAttributeWriter::GetDumpEstimateFactor(
        priority_queue<double>& factors, 
        priority_queue<size_t>& minMemUses) const
{
    if (mAttributeWriter)
    {
        mAttributeWriter->GetDumpEstimateFactor(factors, minMemUses);
    }
}

IE_NAMESPACE_END(index);
