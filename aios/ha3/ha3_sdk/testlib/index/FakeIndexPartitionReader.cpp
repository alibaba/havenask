#include "ha3_sdk/testlib/index/FakeIndexPartitionReader.h"

#include <cstddef>
#include <cstdint>
#include <utility>

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/sorted_docid_range_searcher.h"
#include "indexlib/index_base/index_meta/partition_meta.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib {
namespace partition {
AUTIL_LOG_SETUP(ha3, FakeIndexPartitionReader);

FakeIndexPartitionReader::FakeIndexPartitionReader() {
    vector<uint32_t> segInfo;
    segInfo.push_back(10000);
    updateSegmentInfo(segInfo);
    _lastIncSegmentId = (segmentid_t)0;
}

FakeIndexPartitionReader::~FakeIndexPartitionReader() {}

std::shared_ptr<InvertedIndexReader> FakeIndexPartitionReader::GetInvertedIndexReader() const {
    return _indexReaderPtr;
}

void FakeIndexPartitionReader::SetIndexReader(InvertedIndexReaderPtr indexReaderPtr) {
    _indexReaderPtr = indexReaderPtr;
}

const SummaryReaderPtr &FakeIndexPartitionReader::GetSummaryReader() const {
    return _summaryReaderPtr;
}

void FakeIndexPartitionReader::SetSummaryReader(SummaryReaderPtr summaryReaderPtr) {
    _summaryReaderPtr = summaryReaderPtr;
}

const AttributeReaderPtr &
FakeIndexPartitionReader::GetAttributeReader(const std::string &attributeName) const {
    AttributePtrMap::const_iterator it = _attributeReaders.find(attributeName);
    if (it != _attributeReaders.end()) {
        return it->second;
    } else {
        static AttributeReaderPtr nullPtr;
        return nullPtr;
    }
}

void FakeIndexPartitionReader::AddAttributeReader(const string &attributeName,
                                                  AttributeReaderPtr attrReaderPtr) {
    _attributeReaders.insert(make_pair(attributeName, attrReaderPtr));
}

void FakeIndexPartitionReader::initSortedDocidRangeSearcher() {
    MultiFieldAttributeReaderPtr attrReaders(
        new MultiFieldAttributeReader(indexlib::config::AttributeSchemaPtr(),
                                      nullptr,
                                      config::ReadPreference::RP_RANDOM_PREFER,
                                      0));

    AttributePtrMap::const_iterator iter = _attributeReaders.begin();
    for (size_t i = 0; iter != _attributeReaders.end(); ++iter, ++i) {
        attrReaders->AddAttributeReader(iter->first, iter->second);
    }

    PartitionMeta partitionMeta;
    if (_partInfoPtr->GetPartitionMeta()) {
        partitionMeta = *_partInfoPtr->GetPartitionMeta();
    }

    const indexlibv2::config::SortDescriptions &sortDescs = partitionMeta.GetSortDescriptions();
    vector<string> attributes;
    for (size_t i = 0; i < sortDescs.size(); i++) {
        const string &sortFieldName = sortDescs[i].GetSortFieldName();
        attributes.push_back(sortFieldName);
    }
    _sortedDocIdRangeSearcher.Init(attrReaders, partitionMeta, attributes);
}

void FakeIndexPartitionReader::setPartitionMeta(const PartitionMetaPtr &meta) {
    if (!_partInfoPtr) {
        _partInfoPtr.reset(new PartitionInfo);
    }
    _partInfoPtr->SetPartitionMeta(meta);
}

void FakeIndexPartitionReader::deleteDocuments(const std::string &docIds) {
    StringTokenizer st(
        docIds, ",", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        _deleltionMapReaderPtr->Delete(StringUtil::fromString<docid_t>(st[i].c_str()));
    }
}

void FakeIndexPartitionReader::updateSegmentInfo(const vector<uint32_t> &segmentInfos) {
    indexlib::DocIdRangeVector ranges;
    docid_t baseDocId = 0;
    _baseDocIds.clear();

    for (size_t i = 0; i < segmentInfos.size(); ++i) {
        _baseDocIds.push_back(baseDocId);
        docid_t nextBaseDocId = baseDocId + segmentInfos[i];
        ranges.push_back(indexlib::DocIdRange(baseDocId, nextBaseDocId));
        baseDocId = nextBaseDocId;
    }
    uint32_t docCount = baseDocId;
    _partitionMetrics.totalDocCount = docCount;
    _partInfoPtr.reset(new PartitionInfo);
    _partInfoPtr->SetOrderedDocIdRanges(ranges);

    _deleltionMapReaderPtr.reset(new DeletionMapReader(docCount));
    _partInfoPtr->SetDeletionMapReader(_deleltionMapReaderPtr);
}

} // namespace partition
} // namespace indexlib
