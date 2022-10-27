#include "indexlib/index/normal/sorted_docid_range_searcher.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SortedDocidRangeSearcher);

struct DocIdRangeCompare {
    bool operator() (const DocIdRange& i,
                     const DocIdRange& j) {
        return i.first < j.first;
    }
};

SortedDocidRangeSearcher::SortedDocidRangeSearcher() 
{
}

SortedDocidRangeSearcher::~SortedDocidRangeSearcher() 
{
}

void SortedDocidRangeSearcher::Init(
        const MultiFieldAttributeReaderPtr& attrReaders,
        const PartitionMeta& partitionMeta)
{
    mPartMeta = partitionMeta;
    const SortDescriptions &sortDescs = mPartMeta.GetSortDescriptions();
    for (size_t i = 0; i < sortDescs.size(); i++)
    {
        const string& sortFieldName = sortDescs[i].sortFieldName;
        AttributeReaderPtr sortAttrReader = attrReaders->GetAttributeReader(sortFieldName);
        if (!sortAttrReader)
        {
            INDEXLIB_FATAL_ERROR(NonExist, "GetAttributeReader "
                    "for sort attribute field[%s] failed!", sortFieldName.c_str());
        }
        mSortAttrReaderMap.insert(std::make_pair(sortFieldName, sortAttrReader));
    }
}

bool SortedDocidRangeSearcher::GetSortedDocIdRanges(
        const DimensionDescriptionVector& dimensions,
        const DocIdRange& rangeLimit,
        DocIdRangeVector& resultRanges) const
{
    resultRanges.clear();

    if (dimensions.empty()) 
    {
        resultRanges.push_back(rangeLimit);
        return true;
    }

    if (!ValidateDimensions(dimensions))
    { 
        return false; 
    }

    DocIdRangeVector rangeLimits;
    rangeLimits.push_back(rangeLimit);
    return RecurGetSortedDocIdRanges(dimensions, rangeLimits, resultRanges, 0);
}

bool SortedDocidRangeSearcher::RecurGetSortedDocIdRanges(
        const DimensionDescriptionVector& dimensions,
        DocIdRangeVector& rangeLimits,
        DocIdRangeVector& resultRanges,
        size_t dimensionIdx) const
{
    for (size_t i = 0; i < rangeLimits.size(); ++i)
    {
        DocIdRangeVector pieceRanges;
        if (!GetSortedDocIdRanges(dimensions[dimensionIdx],
                        rangeLimits[i], pieceRanges)) 
        {
            return false;
        }
        AppendRanges(pieceRanges, resultRanges);
    }

    if (resultRanges.empty()) 
    {
        return true;
    }

    if (dimensionIdx + 1 != dimensions.size()) 
    {
        rangeLimits = resultRanges;
        resultRanges.clear();
        if (!RecurGetSortedDocIdRanges(dimensions, rangeLimits,
                        resultRanges, dimensionIdx + 1) ) 
        {
            return false;
        }
    }
    return true;
}

bool SortedDocidRangeSearcher::GetSortedDocIdRanges(
        const DimensionDescriptionPtr& dimension,
        const DocIdRange& rangeLimit,
        DocIdRangeVector& resultRanges) const
{
    const AttributeReaderPtr& attrReader = GetAttributeReader(dimension->name);
    if (!attrReader)
    {
        return false;
    }

    DocIdRange pieceRange;
    for (size_t i = 0; i < dimension->ranges.size(); ++i) 
    {
        if (!attrReader->GetSortedDocIdRange(dimension->ranges[i],
                        rangeLimit, pieceRange)) 
        {
            return false;
        }
        if (pieceRange.second > pieceRange.first) 
        {
            resultRanges.push_back(pieceRange);
        }
    }

    for (size_t i = 0; i < dimension->values.size(); ++i)
    {
        index_base::RangeDescription rangeDescription(dimension->values[i], 
                dimension->values[i]);
        if (!attrReader->GetSortedDocIdRange(
                        rangeDescription, rangeLimit, pieceRange)) 
        {
            return false;
        }
        if (pieceRange.second > pieceRange.first) 
        {
            resultRanges.push_back(pieceRange);
        }
    }

    sort(resultRanges.begin(), resultRanges.end(), DocIdRangeCompare());
    return true;
}

bool SortedDocidRangeSearcher::ValidateDimensions(
        const DimensionDescriptionVector& dimensions) const
{
    size_t count = dimensions.size();
    const SortDescriptions &sortDescs = mPartMeta.GetSortDescriptions();
    if (count > sortDescs.size()
        || dimensions[count-1]->name != sortDescs[count-1].sortFieldName)
    {
        IE_LOG(WARN, "dimensions are inconsistent with partMeta");
        return false;
    }

    for (size_t i = 0; i + 1 < count; ++i)
    {
        if (dimensions[i]->name != sortDescs[i].sortFieldName) 
        {
            IE_LOG(WARN, "dimensions inconsistent with partMeta");
            return false;
        }
        if (!dimensions[i]->ranges.empty()) 
        { 
            IE_LOG(WARN, "high dimension %s can't have range content",
                   dimensions[i]->name.c_str());
            return false; 
        }
    }
    return true;
}

void SortedDocidRangeSearcher::AppendRanges(
        const DocIdRangeVector& src, DocIdRangeVector& dest) const
{
    dest.insert(dest.end(), src.begin(), src.end());
}

const AttributeReaderPtr& SortedDocidRangeSearcher::GetAttributeReader(
        const string& attrName) const
{
    AttributeReaderMap::const_iterator it = 
        mSortAttrReaderMap.find(attrName);
    if (it == mSortAttrReaderMap.end()) 
    {
        static AttributeReaderPtr nullReader;
        return nullReader;
    }
    return it->second;
}

IE_NAMESPACE_END(index);


