#pragma once

#include <cstdint>
#include <map>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeInDocSectionMeta.h"
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/indexlib.h"

namespace indexlib {
namespace index {

class FakeSectionAttributeReader : public SectionAttributeReader {
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

typedef std::shared_ptr<FakeSectionAttributeReader> FakeSectionAttributeReaderPtr;

} // namespace index
} // namespace indexlib
