#ifndef __INDEXLIB_SECTION_ATTRIBUTE_APPENDER_H
#define __INDEXLIB_SECTION_ATTRIBUTE_APPENDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, PackageIndexConfig);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, IndexTokenizeField);
DECLARE_REFERENCE_CLASS(common, SectionAttributeFormatter);
DECLARE_REFERENCE_CLASS(common, StringAttributeConvertor);

IE_NAMESPACE_BEGIN(document);

/**
 * notice: this Class is not thread-safe.
 */
class SectionAttributeAppender
{
public:
    struct IndexMeta
    {
	config::PackageIndexConfigPtr packIndexConfig;
	common::SectionAttributeFormatterPtr formatter;
	common::StringAttributeConvertorPtr convertor;
    };
    typedef std::vector<IndexMeta> IndexMetaVec;
    
public:
    SectionAttributeAppender();
    SectionAttributeAppender(const SectionAttributeAppender& other);
    ~SectionAttributeAppender();
    
public:
    bool Init(const config::IndexPartitionSchemaPtr& schema);
    bool AppendSectionAttribute(const document::IndexDocumentPtr& indexDocument);
    SectionAttributeAppender* Clone();

private:
    void AppendSectionAttributeForOneIndex(
	const IndexMeta& indexMeta, const document::IndexDocumentPtr& indexDocument);

    void AppendSectionAttributeForOneField(
	document::IndexTokenizeField* field, int32_t dGapFid, bool hasFid, bool hasSectionWeight);

    void EncodeSectionAttributeForOneIndex(
	const IndexMeta& indexMeta, const document::IndexDocumentPtr& indexDocument);
    
private:
    IndexMetaVec mIndexMetaVec;
    section_fid_t mSectionFids[MAX_SECTION_COUNT_PER_DOC];
    section_len_t mSectionLens[MAX_SECTION_COUNT_PER_DOC];
    section_weight_t mSectionWeights[MAX_SECTION_COUNT_PER_DOC];
    uint32_t mSectionsCountInCurrentDoc;
    uint32_t mTotalSectionLen;
    bool mSectionOverFlowFlag;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SectionAttributeAppender);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_APPENDER_H
