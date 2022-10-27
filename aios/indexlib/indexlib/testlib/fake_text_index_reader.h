#ifndef __INDEXLIB_FAKE_TEXT_INDEX_READER_H
#define __INDEXLIB_FAKE_TEXT_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/testlib/fake_posting_iterator.h"
#include "indexlib/testlib/fake_section_attribute_reader.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeTextIndexReader : public index::IndexReader
{
public:
    FakeTextIndexReader(const std::string &mpStr);
    FakeTextIndexReader(const std::string &mpStr, 
                        const FakePostingIterator::PositionMap &posMap);
public:
    index::PostingIterator *Lookup(const common::Term& term,
                                   uint32_t statePoolSize = 1000,
                                   PostingType type = pt_default,
                                   autil::mem_pool::Pool *sessionPool = NULL);

    virtual const index::SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const;
    virtual std::string GetIndexName() const {
        return "";
    }
    virtual index::KeyIteratorPtr CreateKeyIterator(const std::string& indexName) {
        assert(false);
        return index::KeyIteratorPtr();
    }
    virtual index::IndexReader* Clone() const {
        assert(false);
        return NULL;
    }
    void setType(const std::string &term, PostingIteratorType type) {
        _typeMap[term] = type;
    }

    void setPostitionMap(const FakePostingIterator::PositionMap &posMap) {
        _posMap = posMap;
    }
    
private:
    FakePostingIterator::Map _map;
    FakePostingIterator::DocSectionMap _docSectionMap;
    FakePostingIterator::IteratorTypeMap _typeMap;
    FakePostingIterator::PositionMap _posMap;
    mutable FakeSectionAttributeReaderPtr _ptr; 
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeTextIndexReader);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_TEXT_INDEX_READER_H
