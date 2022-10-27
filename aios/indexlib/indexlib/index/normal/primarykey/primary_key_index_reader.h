#ifndef __INDEXLIB_PRIMARY_KEY_INDEX_READER_H
#define __INDEXLIB_PRIMARY_KEY_INDEX_READER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"

DECLARE_REFERENCE_CLASS(index, SectionAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyIndexReader : public IndexReader
{
private:
    using IndexReader::Lookup;
public:
    PrimaryKeyIndexReader() {}
    virtual ~PrimaryKeyIndexReader() {}

public:
    virtual void OpenWithoutPKAttribute(
            const config::IndexConfigPtr& indexConfig, 
            const index_base::PartitionDataPtr& partitionData) = 0;

    virtual docid_t Lookup(const std::string &pkStr) const = 0;
    virtual docid_t Lookup(const autil::ConstString& pkStr) const
    { return INVALID_DOCID; }
    virtual docid_t LookupWithPKHash(const autil::uint128_t& pkHash) const = 0;
    virtual docid_t LookupWithNumber(uint64_t pk) const {
        return INVALID_DOCID;
    }
    virtual AttributeReaderPtr GetPKAttributeReader() const = 0;

public:
    // for index_printer, pair<docid, isDeleted>
    virtual bool LookupAll(const std::string& pkStr,
                           std::vector<std::pair<docid_t, bool>>& docidPairVec) const
    { return false; }

private:
    KeyIteratorPtr CreateKeyIterator(const std::string& indexName) override
    { return KeyIteratorPtr(); }

    const SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const override
    { return NULL; }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyIndexReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_INDEX_READER_H
