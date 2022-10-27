#ifndef ISEARCH_FAKEINDEXPARTITIONREADER_H
#define ISEARCH_FAKEINDEXPARTITIONREADER_H

#include <ha3/index/index.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/fake_index_partition_reader_base.h>
#include <ha3_sdk/testlib/index/FakeAttributeReader.h>
#include <ha3_sdk/testlib/index/FakeJoinDocidCacheReader.h>
#include <ha3_sdk/testlib/index/FakeMultiValueAttributeReader.h>
#include <ha3_sdk/testlib/index/FakePrimaryKeyReader.h>
#include <indexlib/index/normal/sorted_docid_range_searcher.h>
#include <indexlib/index/normal/inverted_index/accessor/doc_range_partitioner.h>
#include <map>
#include <string>
#include <ha3_sdk/testlib/index/FakePartitionData.h>

IE_NAMESPACE_BEGIN(partition);

class FakeIndexPartitionReader : public partition::FakeIndexPartitionReaderBase
{
public:
    typedef std::map<std::string, index::AttributeReaderPtr> AttributePtrMap;
public:
    FakeIndexPartitionReader();
    FakeIndexPartitionReader(const IndexPartitionReader &reader) { assert(false); }
    ~FakeIndexPartitionReader();
public:
    /* override */ const index::SummaryReaderPtr &GetSummaryReader() const override;
    void SetSummaryReader(index::SummaryReaderPtr summaryReaderPtr);
    /* override */ const index::AttributeReaderPtr &GetAttributeReader(const std::string& field) const override;
    void AddAttributeReader(const std::string& attributeName, index::AttributeReaderPtr attrReaderPtr);
    /* override */ const index::PackAttributeReaderPtr &GetPackAttributeReader(const std::string& packName) const override
    { assert(false); return _packAttributeReader; }

    /* override */ const index::PackAttributeReaderPtr &GetPackAttributeReader(packattrid_t packId) const override
    { assert(false); return _packAttributeReader; }

    /* override */ index::IndexReaderPtr GetIndexReader() const override;
    void SetIndexReader(index::IndexReaderPtr indexReaderPtr);
    /* override */ const index::IndexReaderPtr &GetIndexReader(const std::string& indexName) const override
    { assert(false); return _indexReaderPtr;}
    /* override */ const index::IndexReaderPtr &GetIndexReader(indexid_t indexId) const override
    { assert(false); return _indexReaderPtr;}
    /* override */ const index::PrimaryKeyIndexReaderPtr &GetPrimaryKeyReader() const override
    {return _primaryKeyIndexReaderPtr;}
    /* override */ const index::DeletionMapReaderPtr &GetDeletionMapReader() const override { return _deleltionMapReaderPtr; }
    void setPrimaryKeyReader(const index::PrimaryKeyIndexReaderPtr &primaryKeyIndexReaderPtr) {
        _primaryKeyIndexReaderPtr = primaryKeyIndexReaderPtr;
    }

    /* override */ index_base::Version GetVersion() const override {return _indexVersion;}
    /* override */ index::AttributeMetrics GetAttributeMetrics() const
    { return index::AttributeMetrics(); }
    void setIndexVersion(const IE_NAMESPACE(index_base)::Version &indexVersion) {
        _indexVersion = indexVersion;
    }
    index::PartitionMetrics GetPartitionMetrics() const {return _partitionMetrics;}
    index::PartitionMetrics& GetPartitionMetrics() {return _partitionMetrics;}
    void setDocCount(uint64_t docCount) { _partitionMetrics.totalDocCount = docCount; }
    void setIncDocCount(uint64_t docCount) { _partitionMetrics.incDocCount = docCount; }
    uint64_t getDocCount() const { return _partitionMetrics.totalDocCount; }
    void setSegmentCount(uint64_t count) { _partitionMetrics.segmentCount = (segmentid_t)count; }
    const std::vector<docid_t> &GetBaseDocIds() const { return _baseDocIds; }
    void SetBaseDocIds(const std::vector<docid_t> &baseDocIds) { _baseDocIds = baseDocIds; }
    uint64_t GetDeletedDocCount() const {return 0;}

    void SetDeletionMapReader(const index::DeletionMapReaderPtr &delMapReader) {
        _deleltionMapReaderPtr = delMapReader;
        _partInfoPtr->SetDeletionMapReader(_deleltionMapReaderPtr);
    }
    bool GetPartedDocIdRanges(const DocIdRangeVector &rangeHint,
                              size_t totalWayCount,
                              std::vector<DocIdRangeVector> &rangeVectors)
        const override
    {
        return index::DocRangePartitioner::GetPartedDocIdRanges(
                rangeHint, _partInfoPtr, totalWayCount, rangeVectors);
    }
    bool GetPartedDocIdRanges(const DocIdRangeVector &rangeHint,
                              size_t totalWayCount, size_t wayIdx,
                              DocIdRangeVector &ranges)
        const override
    {
        return index::DocRangePartitioner::GetPartedDocIdRanges(
                rangeHint, _partInfoPtr, totalWayCount, wayIdx, ranges);
    }
    template<typename T>
    void createFakeAttributeReader(const std::string &name, const std::string &values) {
        index::FakeAttributeReader<T> *fakeAttrReader = new index::FakeAttributeReader<T>;
        fakeAttrReader->setAttributeValues(values);
        _attributeReaders[name] = index::AttributeReaderPtr(fakeAttrReader);
        fakeAttrReader->SetSortType(sp_asc);
    }

    void createFakeJoinDocidAttributeReader(const std::string &name, const std::string &values) {
        index::FakeJoinDocidAttributeReader *fakeAttrReader = new index::FakeJoinDocidAttributeReader;
        fakeAttrReader->setAttributeValues(values);
        _attributeReaders[name] = index::AttributeReaderPtr(fakeAttrReader);
        fakeAttrReader->SetSortType(sp_asc);
    }

    template<typename T>
    void createFakeMultiAttributeReader(const std::string &name, const std::string &values) {
        index::FakeMultiValueAttributeReader<T> *fakeMutilAttrReader = new index::FakeMultiValueAttributeReader<T>;
        fakeMutilAttrReader->setAttributeValues(values);
        _attributeReaders[name] = index::AttributeReaderPtr(fakeMutilAttrReader);
    }

    void createFakeStringAttributeReader(const std::string &name, const std::string &values) {
        index::FakeStringAttributeReader *fakeStringAttrReader = new index::FakeStringAttributeReader;
        fakeStringAttrReader->setAttributeValues(values);
        _attributeReaders[name] = index::AttributeReaderPtr(fakeStringAttrReader);
    }

    template<typename T>
    void createFakePrimaryKeyReader(const std::string &values, const std::string &pkIndexStr = "") {
        index::FakePrimaryKeyReader<T> *fakePKReader = new index::FakePrimaryKeyReader<T>;
        fakePKReader->SetDeletionMapReader(_deleltionMapReaderPtr);
        fakePKReader->LoadFakePrimaryKeyIndex(pkIndexStr);
        fakePKReader->setAttributeValues(values);
        fakePKReader->setPKIndexString(pkIndexStr);
        _primaryKeyIndexReaderPtr.reset(fakePKReader);
    }

    void SetOrderedDocIds(const DocIdRangeVector &ranges) {
        _partInfoPtr->SetOrderedDocIdRanges(ranges);
    }

    void SetUnOrderedDocIdRange(DocIdRange range) {
        _partInfoPtr->SetUnOrderedDocIdRange(range);
    }

    /*override */ index_base::PartitionDataPtr GetPartitionData() const override {
        static partition::FakePartitionDataPtr partitionData(
            new partition::FakePartitionData);
        return partitionData;
    }
    void SetLastIncSegmentId(segmentid_t lastIncSegmentId) {
        _lastIncSegmentId = lastIncSegmentId;
    }
    index::PartitionInfoPtr GetPartitionInfo() const override {
        index::PartitionInfoHint infoHint;
        infoHint.lastIncSegmentId = _lastIncSegmentId;
        _partInfoPtr->SetPartitionInfoHint(infoHint);
        _partInfoPtr->SetBaseDocIds(_baseDocIds);
        _partInfoPtr->GetPartitionMetrics() = _partitionMetrics;
        index_base::Version version;
        for (size_t i = 0;i < _baseDocIds.size(); ++i) {
            version.AddSegment(i);
        }
        _partInfoPtr->SetVersion(version);

        if (mSubIndexPartitionReader)
        {
            _partInfoPtr->SetSubPartitionInfo(
                    mSubIndexPartitionReader->GetPartitionInfo());
        }

        return _partInfoPtr;
    }

    bool GetSortedDocIdRanges(
            const index_base::DimensionDescriptionVector& dimens,
            const DocIdRange& rangeLimits,
            DocIdRangeVector& resultRanges) const override
    {
        return _sortedDocIdRangeSearcher.GetSortedDocIdRanges(
                dimens, rangeLimits, resultRanges);
    }

    void setPartitionMeta(const index_base::PartitionMetaPtr& meta);
    void initSortedDocidRangeSearcher();

    void deleteDocuments(const std::string &docIds);

    const AttributePtrMap& getAttributeReaders() const {
        return _attributeReaders;
    }
    void updateSegmentInfo(const std::vector<uint32_t> &segInfo);
private:
    AttributePtrMap _attributeReaders;
    index::SummaryReaderPtr _summaryReaderPtr;
    index::IndexReaderPtr _indexReaderPtr;
    index::PartitionInfoPtr _partInfoPtr;
    index_base::SegmentInfos _segmentInfos;
    index::PrimaryKeyIndexReaderPtr _primaryKeyIndexReaderPtr;
    index::DeletionMapReaderPtr _deleltionMapReaderPtr;

    mutable IE_NAMESPACE(index)::SortedDocidRangeSearcher _sortedDocIdRangeSearcher;

    IE_NAMESPACE(index_base)::Version _indexVersion;
    std::vector<docid_t> _baseDocIds;
    std::vector<docid_t> _orderedDocIds;
    index::PartitionMetrics  _partitionMetrics;
    segmentid_t _lastIncSegmentId;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FakeIndexPartitionReader);

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEINDEXPARTITIONREADER_H
