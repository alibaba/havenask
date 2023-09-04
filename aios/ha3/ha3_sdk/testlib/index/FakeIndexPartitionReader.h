#pragma once

#include <algorithm>
#include <assert.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3_sdk/testlib/index/FakeAttributeReader.h"
#include "ha3_sdk/testlib/index/FakeJoinDocidCacheReader.h"
#include "ha3_sdk/testlib/index/FakeMultiValueAttributeReader.h"
#include "ha3_sdk/testlib/index/FakePartitionData.h"
#include "ha3_sdk/testlib/index/FakePrimaryKeyReader.h"
#include "ha3_sdk/testlib/index/fake_index_partition_reader_base.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index/normal/sorted_docid_range_searcher.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/doc_range_partitioner.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/table/normal_table/DimensionDescription.h"

namespace indexlib {
namespace partition {

class FakeIndexPartitionReader : public partition::FakeIndexPartitionReaderBase {
public:
    typedef std::map<std::string, indexlib::index::AttributeReaderPtr> AttributePtrMap;

public:
    FakeIndexPartitionReader();
    FakeIndexPartitionReader(const IndexPartitionReader &reader) {
        assert(false);
    }
    ~FakeIndexPartitionReader();

public:
    const indexlib::index::SummaryReaderPtr &GetSummaryReader() const override;
    void SetSummaryReader(indexlib::index::SummaryReaderPtr summaryReaderPtr);
    const indexlib::index::AttributeReaderPtr &
    GetAttributeReader(const std::string &field) const override;
    void AddAttributeReader(const std::string &attributeName,
                            indexlib::index::AttributeReaderPtr attrReaderPtr);
    const indexlib::index::PackAttributeReaderPtr &
    GetPackAttributeReader(const std::string &packName) const override {
        assert(false);
        return _packAttributeReader;
    }

    const indexlib::index::PackAttributeReaderPtr &
    GetPackAttributeReader(packattrid_t packId) const override {
        assert(false);
        return _packAttributeReader;
    }

    std::shared_ptr<indexlib::index::InvertedIndexReader> GetInvertedIndexReader() const override;
    void SetIndexReader(indexlib::index::InvertedIndexReaderPtr indexReaderPtr);
    const indexlib::index::InvertedIndexReaderPtr &
    GetInvertedIndexReader(const std::string &indexName) const override {
        assert(false);
        return _indexReaderPtr;
    }
    const indexlib::index::InvertedIndexReaderPtr &
    GetInvertedIndexReader(indexid_t indexId) const override {
        assert(false);
        return _indexReaderPtr;
    }
    const indexlib::index::PrimaryKeyIndexReaderPtr &GetPrimaryKeyReader() const override {
        return _primaryKeyIndexReaderPtr;
    }
    const indexlib::index::DeletionMapReaderPtr &GetDeletionMapReader() const override {
        return _deleltionMapReaderPtr;
    }
    void
    setPrimaryKeyReader(const indexlib::index::PrimaryKeyIndexReaderPtr &primaryKeyIndexReaderPtr) {
        _primaryKeyIndexReaderPtr = primaryKeyIndexReaderPtr;
    }

    index_base::Version GetVersion() const override {
        return _indexVersion;
    }
    indexlib::index::AttributeMetrics GetAttributeMetrics() const {
        return indexlib::index::AttributeMetrics();
    }
    void setIndexVersion(const indexlib::index_base::Version &indexVersion) {
        _indexVersion = indexVersion;
    }
    indexlib::index::PartitionMetrics GetPartitionMetrics() const {
        return _partitionMetrics;
    }
    indexlib::index::PartitionMetrics &GetPartitionMetrics() {
        return _partitionMetrics;
    }
    void setDocCount(uint64_t docCount) {
        _partitionMetrics.totalDocCount = docCount;
    }
    void setIncDocCount(uint64_t docCount) {
        _partitionMetrics.incDocCount = docCount;
    }
    uint64_t getDocCount() const {
        return _partitionMetrics.totalDocCount;
    }
    void setSegmentCount(uint64_t count) {
        _partitionMetrics.segmentCount = (segmentid_t)count;
    }
    const std::vector<docid_t> &GetBaseDocIds() const {
        return _baseDocIds;
    }
    void SetBaseDocIds(const std::vector<docid_t> &baseDocIds) {
        _baseDocIds = baseDocIds;
    }
    uint64_t GetDeletedDocCount() const {
        return 0;
    }

    void SetDeletionMapReader(const indexlib::index::DeletionMapReaderPtr &delMapReader) {
        _deleltionMapReaderPtr = delMapReader;
        _partInfoPtr->SetDeletionMapReader(_deleltionMapReaderPtr);
    }
    bool
    GetPartedDocIdRanges(const indexlib::DocIdRangeVector &rangeHint,
                         size_t totalWayCount,
                         std::vector<indexlib::DocIdRangeVector> &rangeVectors) const override {
        return indexlib::partition::DocRangePartitioner::GetPartedDocIdRanges(
            rangeHint, _partInfoPtr, totalWayCount, rangeVectors);
    }
    bool GetPartedDocIdRanges(const indexlib::DocIdRangeVector &rangeHint,
                              size_t totalWayCount,
                              size_t wayIdx,
                              indexlib::DocIdRangeVector &ranges) const override {
        return indexlib::partition::DocRangePartitioner::GetPartedDocIdRanges(
            rangeHint, _partInfoPtr, totalWayCount, wayIdx, ranges);
    }
    template <typename T>
    void createFakeAttributeReader(const std::string &name, const std::string &values) {
        indexlib::index::FakeAttributeReader<T> *fakeAttrReader
            = new indexlib::index::FakeAttributeReader<T>;
        fakeAttrReader->setAttributeValues(values);
        _attributeReaders[name] = indexlib::index::AttributeReaderPtr(fakeAttrReader);
        fakeAttrReader->SetSortType(indexlibv2::config::sp_asc);
    }

    void createFakeJoinDocidAttributeReader(const std::string &name, const std::string &values) {
        indexlib::index::FakeJoinDocidAttributeReader *fakeAttrReader
            = new indexlib::index::FakeJoinDocidAttributeReader;
        fakeAttrReader->setAttributeValues(values);
        _attributeReaders[name] = indexlib::index::AttributeReaderPtr(fakeAttrReader);
        fakeAttrReader->SetSortType(indexlibv2::config::sp_asc);
    }

    template <typename T>
    void createFakeMultiAttributeReader(const std::string &name, const std::string &values) {
        indexlib::index::FakeMultiValueAttributeReader<T> *fakeMutilAttrReader
            = new indexlib::index::FakeMultiValueAttributeReader<T>;
        fakeMutilAttrReader->setAttributeValues(values);
        _attributeReaders[name] = indexlib::index::AttributeReaderPtr(fakeMutilAttrReader);
    }

    void createFakeStringAttributeReader(const std::string &name, const std::string &values) {
        indexlib::index::FakeStringAttributeReader *fakeStringAttrReader
            = new indexlib::index::FakeStringAttributeReader;
        fakeStringAttrReader->setAttributeValues(values);
        _attributeReaders[name] = indexlib::index::AttributeReaderPtr(fakeStringAttrReader);
    }

    template <typename T>
    void createFakePrimaryKeyReader(const std::string &values, const std::string &pkIndexStr = "") {
        indexlib::index::FakePrimaryKeyReader<T> *fakePKReader
            = new indexlib::index::FakePrimaryKeyReader<T>;
        fakePKReader->SetDeletionMapReader(_deleltionMapReaderPtr);
        fakePKReader->LoadFakePrimaryKeyIndex(pkIndexStr);
        fakePKReader->setAttributeValues(values);
        fakePKReader->setPKIndexString(pkIndexStr);
        _primaryKeyIndexReaderPtr.reset(fakePKReader);
    }

    void SetOrderedDocIds(const indexlib::DocIdRangeVector &ranges) {
        _partInfoPtr->SetOrderedDocIdRanges(ranges);
    }

    void SetUnOrderedDocIdRange(indexlib::DocIdRange range) {
        _partInfoPtr->SetUnOrderedDocIdRange(range);
    }

    /*override */ index_base::PartitionDataPtr GetPartitionData() const override {
        static partition::FakePartitionDataPtr partitionData(new partition::FakePartitionData);
        return partitionData;
    }
    void SetLastIncSegmentId(segmentid_t lastIncSegmentId) {
        _lastIncSegmentId = lastIncSegmentId;
    }
    indexlib::index::PartitionInfoPtr GetPartitionInfo() const override {
        indexlib::index::PartitionInfoHint infoHint;
        infoHint.lastIncSegmentId = _lastIncSegmentId;
        _partInfoPtr->SetPartitionInfoHint(infoHint);
        _partInfoPtr->SetBaseDocIds(_baseDocIds);
        _partInfoPtr->GetPartitionMetrics() = _partitionMetrics;
        index_base::Version version;
        for (size_t i = 0; i < _baseDocIds.size(); ++i) {
            version.AddSegment(i);
        }
        _partInfoPtr->SetVersion(version);

        if (mSubIndexPartitionReader) {
            _partInfoPtr->SetSubPartitionInfo(mSubIndexPartitionReader->GetPartitionInfo());
        }

        return _partInfoPtr;
    }

    bool GetSortedDocIdRanges(const indexlib::table::DimensionDescriptionVector &dimens,
                              const indexlib::DocIdRange &rangeLimits,
                              indexlib::DocIdRangeVector &resultRanges) const override {
        return _sortedDocIdRangeSearcher.GetSortedDocIdRanges(dimens, rangeLimits, resultRanges);
    }

    void setPartitionMeta(const index_base::PartitionMetaPtr &meta);
    void initSortedDocidRangeSearcher();

    void deleteDocuments(const std::string &docIds);

    const AttributePtrMap &getAttributeReaders() const {
        return _attributeReaders;
    }
    void updateSegmentInfo(const std::vector<uint32_t> &segInfo);

private:
    AttributePtrMap _attributeReaders;
    indexlib::index::SummaryReaderPtr _summaryReaderPtr;
    indexlib::index::InvertedIndexReaderPtr _indexReaderPtr;
    indexlib::index::PartitionInfoPtr _partInfoPtr;
    index_base::SegmentInfos _segmentInfos;
    indexlib::index::PrimaryKeyIndexReaderPtr _primaryKeyIndexReaderPtr;
    indexlib::index::DeletionMapReaderPtr _deleltionMapReaderPtr;

    mutable indexlib::index::SortedDocidRangeSearcher _sortedDocIdRangeSearcher;

    indexlib::index_base::Version _indexVersion;
    std::vector<docid_t> _baseDocIds;
    std::vector<docid_t> _orderedDocIds;
    indexlib::index::PartitionMetrics _partitionMetrics;
    segmentid_t _lastIncSegmentId;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FakeIndexPartitionReader> FakeIndexPartitionReaderPtr;

} // namespace partition
} // namespace indexlib
