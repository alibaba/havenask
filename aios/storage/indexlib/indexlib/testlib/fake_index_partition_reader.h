#ifndef __INDEXLIB_FAKE_INDEX_PARTITION_READER_H
#define __INDEXLIB_FAKE_INDEX_PARTITION_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/testlib/fake_attribute_reader.h"
#include "indexlib/testlib/fake_multi_value_attribute_reader.h"
#include "indexlib/testlib/fake_primary_key_index_reader.h"

namespace indexlib { namespace testlib {

class FakeIndexPartitionReader : public partition::IndexPartitionReader
{
public:
    FakeIndexPartitionReader();
    ~FakeIndexPartitionReader();

public:
    void Open(const index_base::PartitionDataPtr& partitionData) override;

    const index::SummaryReaderPtr& GetSummaryReader() const override;
    const index::SourceReaderPtr& GetSourceReader() const override;
    const index::AttributeReaderPtr& GetAttributeReader(const std::string& field) const override;
    const index::PackAttributeReaderPtr& GetPackAttributeReader(const std::string& packAttrName) const override;

    const index::PackAttributeReaderPtr& GetPackAttributeReader(packattrid_t packAttrId) const override;

    const index::AttributeReaderContainerPtr& GetAttributeReaderContainer() const override
    {
        assert(false);
        return index::AttributeReaderContainer::RET_EMPTY_ATTR_READER_CONTAINER;
    }

    std::shared_ptr<index::InvertedIndexReader> GetInvertedIndexReader() const override;
    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(const std::string& indexName) const override;
    const index::InvertedIndexReaderPtr& GetInvertedIndexReader(indexid_t indexId) const override;

    const index::PrimaryKeyIndexReaderPtr& GetPrimaryKeyReader() const override;

    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;

    const index::DeletionMapReaderPtr& GetDeletionMapReader() const override;

    index::PartitionInfoPtr GetPartitionInfo() const override;

    partition::IndexPartitionReaderPtr GetSubPartitionReader() const override;
    versionid_t GetIncVersionId() const override { return 0; }
    void SetSchema(const config::IndexPartitionSchemaPtr& schema) { mSchema = schema; }
    bool GetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions, const DocIdRange& rangeLimits,
                              DocIdRangeVector& resultRanges) const override;

    const AccessCounterMap& GetIndexAccessCounters() const override;

    const AccessCounterMap& GetAttributeAccessCounters() const override;

    void setSegmentCount(uint64_t count) { mPartitionMetrics.segmentCount = (segmentid_t)count; }
    void setDocCount(uint64_t docCount) { mPartitionMetrics.totalDocCount = docCount; }
    void setIncDocCount(uint64_t docCount) { mPartitionMetrics.incDocCount = docCount; }
    uint64_t getDocCount() const { return mPartitionMetrics.totalDocCount; }
    void SetBaseDocIds(const std::vector<docid_t>& baseDocIds) { mBaseDocIds = baseDocIds; }
    void SetOrderedDocIds(const DocIdRangeVector& ranges) { mPartInfoPtr->SetOrderedDocIdRanges(ranges); }
    void SetUnOrderedDocIdRange(DocIdRange range) { mPartInfoPtr->SetUnOrderedDocIdRange(range); }
    void SetDeletionMapReader(const index::DeletionMapReaderPtr& delMapReader)
    {
        mDeletionMapReaderPtr = delMapReader;
        mPartInfoPtr->SetDeletionMapReader(mDeletionMapReaderPtr);
    }

public:
    // for creator
    void SetVersion(const index_base::Version& version);
    void SetIndexReader(const index::LegacyIndexReaderPtr& indexReader);
    void SetSummaryReader(const index::SummaryReaderPtr& summaryReader);

    template <typename T>
    void createFakeAttributeReader(const std::string& name, const std::string& values)
    {
        FakeAttributeReader<T>* fakeAttrReader = new FakeAttributeReader<T>;
        fakeAttrReader->setAttributeValues(values);
        mAttributeReaders[name] = index::AttributeReaderPtr(fakeAttrReader);
    }
    void createFakeJoinDocidAttributeReader(const std::string& name, const std::string& values)
    {
        createFakeAttributeReader<int32_t>(name, values);
    }

    template <typename T>
    void createFakeMultiAttributeReader(const std::string& name, const std::string& values, const std::string& rootPath)
    {
        FakeMultiValueAttributeReader<T>* fakeMutilAttrReader = new FakeMultiValueAttributeReader<T>(rootPath);
        fakeMutilAttrReader->setAttributeValues(values);
        mAttributeReaders[name] = index::AttributeReaderPtr(fakeMutilAttrReader);
    }

    void createFakeStringAttributeReader(const std::string& name, const std::string& values,
                                         const std::string& rootPath, const std::string& stringValueSep)
    {
        FakeStringAttributeReader* fakeStringAttrReader = new FakeStringAttributeReader(rootPath);
        fakeStringAttrReader->setAttributeValues(values, stringValueSep);
        mAttributeReaders[name] = index::AttributeReaderPtr(fakeStringAttrReader);
    }
    template <typename T>
    void createFakePrimaryKeyReader(const std::string& values, const std::string& rootPath,
                                    const std::string& pkIndexStr = "",
                                    index::PrimaryKeyHashType hashType = index::pk_number_hash)
    {
        FakePrimaryKeyIndexReader<T>* fakePKReader = new FakePrimaryKeyIndexReader<T>(hashType);
        fakePKReader->SetDeletionMapReader(mDeletionMapReaderPtr);
        fakePKReader->LoadFakePrimaryKeyIndex(pkIndexStr, rootPath);
        fakePKReader->setAttributeValues(values);
        fakePKReader->setPKIndexString(pkIndexStr);
        mPKIndexReaderPtr.reset(fakePKReader);
    }

private:
    std::map<std::string, index::AttributeReaderPtr> mAttributeReaders;
    index::SummaryReaderPtr mSummaryReaderPtr;
    index::SourceReaderPtr mSourceReaderPtr;
    index::InvertedIndexReaderPtr mIndexReaderPtr;
    index::PartitionInfoPtr mPartInfoPtr;
    index::PrimaryKeyIndexReaderPtr mPKIndexReaderPtr;
    index::DeletionMapReaderPtr mDeletionMapReaderPtr;
    index_base::Version mVersion;

    AccessCounterMap mIndexAccessCounter;
    AccessCounterMap mAttributeAccessCounter;

    partition::IndexPartitionReaderPtr mSubIndexPartitionReader;
    std::vector<docid_t> mBaseDocIds;
    index::PartitionMetrics mPartitionMetrics;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeIndexPartitionReader);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_INDEX_PARTITION_READER_H
