#ifndef ISEARCH_INDEX_FAKESECTIONATTRIBUTEREADER_H
#define ISEARCH_INDEX_FAKESECTIONATTRIBUTEREADER_H

#include <ha3/index/index.h>
#include <vector>
#include <map>
#include <indexlib/common_define.h>
#include <ha3/isearch.h>
#include <indexlib/index/normal/inverted_index/accessor/section_attribute_reader.h>
#include <ha3_sdk/testlib/index/FakeInDocSectionMeta.h>
#include <ha3/index/index.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>


IE_NAMESPACE_BEGIN(index);

class FakeSectionAttributeReader : public SectionAttributeReader
{
public:
    FakeSectionAttributeReader(
        const std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta> &sectionMap) {
        _sectionAttributeMap = sectionMap;
    }
    FakeSectionAttributeReader() {}
    virtual ~FakeSectionAttributeReader() {
        _sectionAttributeMap.clear();
    }
public:
    virtual InDocSectionMetaPtr GetSection(docid_t docId) const;

    /* virtual void Open(const storage::StoragePtr& partition, */
    /*                   const SegmentInfos& segmentInfos, fieldid_t fieldId) {assert(false);} */
    /* virtual void ReOpen(const SegmentInfos& segmentInfos) {assert(false);} */

    /* virtual SectionAttributeReader* Clone() const {assert(false); return NULL;} */

private:
    std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta> _sectionAttributeMap;
};

typedef std::tr1::shared_ptr<FakeSectionAttributeReader> FakeSectionAttributeReaderPtr;

IE_NAMESPACE_END(index);
#endif
