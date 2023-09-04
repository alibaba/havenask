#ifndef __INDEXLIB_INDEX_PARTITION_READER_MOCK_H
#define __INDEXLIB_INDEX_PARTITION_READER_MOCK_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/index_partition_reader_impl.h"
#include "indexlib/index/multi_field_index_reader.h"
#include "indexlib/index/test/fake_index_reader_base.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/test/fake_attribute_reader.h"
#include "indexlib/partition/test/fake_summary_reader.h"

namespace indexlib { namespace partition {

const static versionid_t BAD_VERSION_OF_INDEX = -1988;

class FakePrimaryKeyReader : public PrimaryKeyIndexReader
{
public:
    FakePrimaryKeyReader() : PrimaryKeyIndexReader() {}
    FakePrimaryKeyReader(const FakePrimaryKeyReader& other) {}
    PostingIterator* Lookup(const index::Term& term, uint32_t statePoolSize = 1000, PostingType type = pt_default,
                            autil::mem_pool::Pool* sessionPool = NULL)
    {
        return NULL;
    }

    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const { return NULL; }

    std::string GetIdentifier() const { return "fake_index_reader_base"; }
    std::string GetIndexName() const { return ""; }

    void SetIndexConfig(const config::IndexConfigPtr& indexConfig) {}

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName)
    {
        return std::shared_ptr<KeyIterator>();
    }

    bool ReOpen(const SegmentDirectoryPtr& segDir)
    {
        if (segDir->GetVersion().GetVersionId() == BAD_VERSION_OF_INDEX) {
            return false;
        }
        return true;
    }

    InvertedIndexReader* Clone() const { return new FakePrimaryKeyReader(*this); }

    void AddIndexSegmentReader(const index::IndexSegmentReaderPtr& indexSegmentReader) {}

    void SetDocOperateAttrReader(const AttributeReaderPtr& attrReader) {}

    docid_t Lookup(const std::string& pkStr) const { return INVALID_DOCID; }

    docid_t LookupWithPKHash(const autil::uint128_t& pkHash, future_lite::Executor* executor) const
    {
        return INVALID_DOCID;
    }

    docid_t LookupWithSegId(const std::string& pkStr, segmentid_t segId) const { return INVALID_DOCID; }

    AttributeReaderPtr GetPKAttributeReader() const { return AttributeReaderPtr(); }
};

class FakeIndexReaderForIPR : public FakeIndexReaderBase
{
public:
    FakeIndexReaderForIPR() {}
    FakeIndexReaderForIPR(const FakeIndexReaderForIPR& reader) : FakeIndexReaderBase(reader) {}
    ~FakeIndexReaderForIPR() {}

public:
    bool ReOpen(const SegmentDirectoryPtr& segDir)
    {
        if (segDir->GetVersion().GetVersionId() == BAD_VERSION_OF_INDEX) {
            return false;
        }
        return true;
    }

    InvertedIndexReader* Clone() const { return new FakeIndexReaderForIPR(*this); }

    virtual void AddIndexSegmentReader(const index::IndexSegmentReaderPtr& indexSegmentReader) {}
};

DEFINE_SHARED_PTR(FakeIndexReaderForIPR);

typedef FakeAttributeReader FakeAttributeReaderForIPR;

typedef FakeSummaryReader FakeSummaryReaderForIPR;

DEFINE_SHARED_PTR(FakeSummaryReaderForIPR);
}} // namespace indexlib::partition

#endif //__INDEXLIB_INDEX_PARTITION_READER_MOCK_H
