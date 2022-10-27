#ifndef __INDEXLIB_SORTED_DOCID_RANGE_SEARCHER_H
#define __INDEXLIB_SORTED_DOCID_RANGE_SEARCHER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/dimension_description.h"
#include "indexlib/index_base/index_meta/partition_meta.h"

DECLARE_REFERENCE_CLASS(index, MultiFieldAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);

IE_NAMESPACE_BEGIN(index);

class SortedDocidRangeSearcher
{
public:
    SortedDocidRangeSearcher();
    ~SortedDocidRangeSearcher();

public:
    void Init(const MultiFieldAttributeReaderPtr& attrReaders,
              const index_base::PartitionMeta& partitionMeta);

    bool GetSortedDocIdRanges(const index_base::DimensionDescriptionVector& dimensions,
                              const DocIdRange& rangeLimit,
                              DocIdRangeVector& resultRanges) const;
private:
    bool RecurGetSortedDocIdRanges(
            const index_base::DimensionDescriptionVector& dimensions,
            DocIdRangeVector& rangeLimits,
            DocIdRangeVector& resultRanges,
            size_t dimensionIdx) const;

    bool GetSortedDocIdRanges(
            const index_base::DimensionDescriptionPtr& dimension,
            const DocIdRange& rangeLimit,
            DocIdRangeVector& resultRanges) const;
    void AppendRanges(
            const DocIdRangeVector& src, DocIdRangeVector& dest) const;

    bool ValidateDimensions(const index_base::DimensionDescriptionVector& dimensions) const;
    const AttributeReaderPtr& GetAttributeReader(const std::string& attrName) const;

private:
    typedef std::tr1::unordered_map<std::string, AttributeReaderPtr> AttributeReaderMap;

    AttributeReaderMap mSortAttrReaderMap;
    index_base::PartitionMeta mPartMeta;

private:
    IE_LOG_DECLARE();
    friend class SortedDocidRangeSearcherTest;
};

DEFINE_SHARED_PTR(SortedDocidRangeSearcher);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SORTED_DOCID_RANGE_SEARCHER_H
