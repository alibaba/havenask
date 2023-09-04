#include "indexlib/testlib/fake_index_partition_reader.h"

#include "indexlib/index/normal/framework/legacy_index_reader.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::index;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeIndexPartitionReader);

FakeIndexPartitionReader::FakeIndexPartitionReader()
{
    mPartInfoPtr.reset(new PartitionInfo);
    mSchema.reset(new IndexPartitionSchema);
    mSchema->SetTableType("");
}

FakeIndexPartitionReader::~FakeIndexPartitionReader() {}

void FakeIndexPartitionReader::Open(const index_base::PartitionDataPtr& partitionData)
{
    // do nothing
}

const SummaryReaderPtr& FakeIndexPartitionReader::GetSummaryReader() const { return mSummaryReaderPtr; }

const SourceReaderPtr& FakeIndexPartitionReader::GetSourceReader() const { return mSourceReaderPtr; }

const AttributeReaderPtr& FakeIndexPartitionReader::GetAttributeReader(const std::string& attributeName) const
{
    map<string, AttributeReaderPtr>::const_iterator it = mAttributeReaders.find(attributeName);
    if (it != mAttributeReaders.end()) {
        return it->second;
    } else {
        return AttributeReaderContainer::RET_EMPTY_ATTR_READER;
        ;
    }
}

const PackAttributeReaderPtr& FakeIndexPartitionReader::GetPackAttributeReader(const std::string& packAttrName) const
{
    return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
}

const PackAttributeReaderPtr& FakeIndexPartitionReader::GetPackAttributeReader(packattrid_t packAttrId) const
{
    return AttributeReaderContainer::RET_EMPTY_PACK_ATTR_READER;
}

std::shared_ptr<InvertedIndexReader> FakeIndexPartitionReader::GetInvertedIndexReader() const
{
    return mIndexReaderPtr;
}

const InvertedIndexReaderPtr& FakeIndexPartitionReader::GetInvertedIndexReader(const std::string& indexName) const
{
    return mIndexReaderPtr;
}

const InvertedIndexReaderPtr& FakeIndexPartitionReader::GetInvertedIndexReader(indexid_t indexId) const
{
    return mIndexReaderPtr;
}

const PrimaryKeyIndexReaderPtr& FakeIndexPartitionReader::GetPrimaryKeyReader() const { return mPKIndexReaderPtr; }

Version FakeIndexPartitionReader::GetVersion() const { return mVersion; }

Version FakeIndexPartitionReader::GetOnDiskVersion() const { return mVersion; }

const DeletionMapReaderPtr& FakeIndexPartitionReader::GetDeletionMapReader() const { return mDeletionMapReaderPtr; }

index::PartitionInfoPtr FakeIndexPartitionReader::GetPartitionInfo() const { return mPartInfoPtr; }

IndexPartitionReaderPtr FakeIndexPartitionReader::GetSubPartitionReader() const { return mSubIndexPartitionReader; }

bool FakeIndexPartitionReader::GetSortedDocIdRanges(const table::DimensionDescriptionVector& dimensions,
                                                    const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const
{
    // not support yet!
    return false;
}

const partition::IndexPartitionReader::AccessCounterMap& FakeIndexPartitionReader::GetIndexAccessCounters() const
{
    return mIndexAccessCounter;
}

const partition::IndexPartitionReader::AccessCounterMap& FakeIndexPartitionReader::GetAttributeAccessCounters() const
{
    return mAttributeAccessCounter;
}

void FakeIndexPartitionReader::SetVersion(const Version& version) { mVersion = version; }

void FakeIndexPartitionReader::SetSummaryReader(const SummaryReaderPtr& summaryReader)
{
    mSummaryReaderPtr = summaryReader;
}

void FakeIndexPartitionReader::SetIndexReader(const LegacyIndexReaderPtr& indexReader)
{
    mIndexReaderPtr = indexReader;
}
}} // namespace indexlib::testlib
