#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeAttributeReader.h>
#include <indexlib/index/normal/attribute/accessor/multi_field_attribute_reader.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
HA3_LOG_SETUP(partition, FakeIndexPartitionReader);

FakeIndexPartitionReader::FakeIndexPartitionReader() {
    vector<uint32_t> segInfo;
    segInfo.push_back(10000);
    updateSegmentInfo(segInfo);
    _lastIncSegmentId = (segmentid_t)0;
}

FakeIndexPartitionReader::~FakeIndexPartitionReader() { 
}

IndexReaderPtr FakeIndexPartitionReader::GetIndexReader() const {
    return _indexReaderPtr;
}

void FakeIndexPartitionReader::SetIndexReader(IndexReaderPtr indexReaderPtr) {
    _indexReaderPtr = indexReaderPtr;
}

const SummaryReaderPtr &FakeIndexPartitionReader::GetSummaryReader() const {
    return _summaryReaderPtr;
}

void FakeIndexPartitionReader::SetSummaryReader(SummaryReaderPtr summaryReaderPtr) {
    _summaryReaderPtr = summaryReaderPtr;
}


const AttributeReaderPtr &FakeIndexPartitionReader::GetAttributeReader(
        const std::string& attributeName) const
{
    AttributePtrMap::const_iterator it = _attributeReaders.find(attributeName);
    if (it != _attributeReaders.end()) {
        return it->second;
    } else {
        static AttributeReaderPtr nullPtr;
        return nullPtr;
    }
}

void FakeIndexPartitionReader::
AddAttributeReader(const string& attributeName, AttributeReaderPtr attrReaderPtr) 
{
    _attributeReaders.insert(make_pair(attributeName, attrReaderPtr));
}

void FakeIndexPartitionReader::initSortedDocidRangeSearcher()
{
    MultiFieldAttributeReaderPtr attrReaders(new MultiFieldAttributeReader(
                    IE_NAMESPACE(config)::AttributeSchemaPtr()));

    AttributePtrMap::const_iterator iter = _attributeReaders.begin();
    for (size_t i = 0; iter != _attributeReaders.end(); ++iter, ++i)
    {
        attrReaders->AddAttributeReader(iter->first, iter->second);
    }

    PartitionMeta partitionMeta;
    if (_partInfoPtr->GetPartitionMeta())
    {
        partitionMeta = *_partInfoPtr->GetPartitionMeta();
    }

    
    _sortedDocIdRangeSearcher.Init(attrReaders, partitionMeta);
}

void FakeIndexPartitionReader::setPartitionMeta(
        const PartitionMetaPtr& meta) {
    if (!_partInfoPtr) {
        _partInfoPtr.reset(new PartitionInfo);
    }
    _partInfoPtr->SetPartitionMeta(meta);
}

void FakeIndexPartitionReader::deleteDocuments(const std::string &docIds) {
    StringTokenizer st(docIds, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0;i < st.getNumTokens(); ++i) {
        _deleltionMapReaderPtr->Delete(StringUtil::fromString<docid_t>(st[i].c_str()));
    }
}

void FakeIndexPartitionReader::updateSegmentInfo(const vector<uint32_t> &segmentInfos) {
    DocIdRangeVector ranges;
    docid_t baseDocId = 0;
    _baseDocIds.clear();

    for (size_t i = 0; i < segmentInfos.size(); ++i) {
        _baseDocIds.push_back(baseDocId);
        docid_t nextBaseDocId = baseDocId + segmentInfos[i];
        ranges.push_back(DocIdRange(baseDocId, nextBaseDocId));
        baseDocId = nextBaseDocId;
    }
    uint32_t docCount = baseDocId;
    _partitionMetrics.totalDocCount = docCount;
    _partInfoPtr.reset(new PartitionInfo);
    _partInfoPtr->SetOrderedDocIdRanges(ranges);

    _deleltionMapReaderPtr.reset(new DeletionMapReader(docCount));
    _partInfoPtr->SetDeletionMapReader(_deleltionMapReaderPtr);
}

IE_NAMESPACE_END(partition);

