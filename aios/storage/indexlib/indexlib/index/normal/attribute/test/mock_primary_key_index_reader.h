#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"

namespace indexlib { namespace index {

class MockPrimaryKeyIndexReader : public PrimaryKeyIndexReader
{
public:
    MockPrimaryKeyIndexReader(std::map<std::string, docid_t> pkToDocIdMap)
        : PrimaryKeyIndexReader()
        , _pkToDocIdMap(pkToDocIdMap)
    {
    }
    ~MockPrimaryKeyIndexReader() {}

public:
    MockPrimaryKeyIndexReader(const MockPrimaryKeyIndexReader&) = delete;
    MockPrimaryKeyIndexReader& operator=(const MockPrimaryKeyIndexReader&) = delete;
    MockPrimaryKeyIndexReader(MockPrimaryKeyIndexReader&&) = delete;
    MockPrimaryKeyIndexReader& operator=(MockPrimaryKeyIndexReader&&) = delete;

public:
    docid_t Lookup(const std::string& pkStr) const override
    {
        if (_pkToDocIdMap.find(pkStr) != _pkToDocIdMap.end()) {
            return _pkToDocIdMap.at(pkStr);
        }
        return INVALID_DOCID;
    }

    void OpenWithoutPKAttribute(const config::IndexConfigPtr& indexConfig,
                                const index_base::PartitionDataPtr& partitionData) override
    {
        assert(false);
    }

    docid_t LookupWithPKHash(const autil::uint128_t& pkHash) const override
    {
        assert(false);
        return INVALID_DOCID;
    }
    bool LookupWithPKHash(const autil::uint128_t& pkHash, segmentid_t specifySegment, docid_t* docid) const override
    {
        assert(false);
        return INVALID_DOCID;
    }
    AttributeReaderPtr GetPKAttributeReader() const override
    {
        assert(false);
        return nullptr;
    }
    PostingIterator* Lookup(const index::Term& term, uint32_t statePoolSize = 1000, PostingType type = pt_default,
                            autil::mem_pool::Pool* sessionPool = nullptr) override
    {
        assert(false);
        return nullptr;
    }

private:
    std::map<std::string, docid_t> _pkToDocIdMap;
};

}} // namespace indexlib::index
