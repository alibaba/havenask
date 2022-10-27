#ifndef __INDEXLIB_FAKE_INDEX_READER_H
#define __INDEXLIB_FAKE_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/testlib/fake_text_index_reader.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeIndexReader : public index::IndexReader
{
public:
    FakeIndexReader() {
    }
    ~FakeIndexReader() {
    }
public:
    index::IndexReader* Clone() const override{
        assert(false);
        return NULL;
    }
    virtual std::string GetIndexName() const {
        assert(false);
        return "";
    }
    /**
     * Get section reader 
     */
    const index::SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const override
    {
        std::map<std::string, index::SectionAttributeReaderPtr>::const_iterator it
            = _sectionReaders.find(indexName);
        if (_sectionReaders.end() == it) {
            return NULL;
        }
        return it->second.get();
    }

    void addSectionReader(const std::string &indexName, 
                          index::SectionAttributeReaderPtr readerPtr) 
    {
        _sectionReaders.insert(make_pair(indexName, readerPtr));
    }

    float GetCacheHitRatio(const std::string& indexName) const { return 0.0; }

    index::PostingIterator *Lookup(const common::Term& term, uint32_t statePoolSize = 1000,
                                   PostingType type = pt_default,
                                   autil::mem_pool::Pool *pool = NULL) override
    {
        if (type == pt_bitmap) {
            std::string indexName = "bitmap_" + term.GetIndexName();
            std::map<std::string, index::IndexReaderPtr>::iterator it = _indexReaders.find(indexName);
            if (it == _indexReaders.end()) {
                return NULL;
            }
            return it->second->Lookup(term, statePoolSize, type, pool);
        }
        index::PostingIterator *iter = NULL;
        std::map<std::string, index::IndexReaderPtr>::iterator it =
            _indexReaders.find(term.GetTruncateName());
        if (it != _indexReaders.end()) {
            iter = it->second->Lookup(term, statePoolSize, type, pool);
        }
        index::PostingIterator *mainChainIter = NULL;
        it = _indexReaders.find(term.GetIndexName());
        if (it != _indexReaders.end()) {
            mainChainIter = it->second->Lookup(term, statePoolSize, type, pool);
        }
        index::PostingIterator *retIter = mainChainIter;
        if (iter) {
            FakePostingIterator *fakeIter = dynamic_cast<FakePostingIterator*>(iter);
            if (fakeIter && mainChainIter) {
                fakeIter->setTermMeta(mainChainIter->GetTermMeta());
            }
            POOL_COMPATIBLE_DELETE_CLASS(pool, mainChainIter);
            retIter = iter;
        }
        FakePostingIterator *fakeIter = dynamic_cast<FakePostingIterator*>(retIter);
        if (fakeIter && term.GetIndexName().find("expack_") != std::string::npos) {
            fakeIter->setType(pi_buffered);
        }        
        return retIter;
    }

    void addIndexReader(const std::string &indexName, index::IndexReader *reader) {
        index::IndexReaderPtr ptr(reader);
        _indexReaders.insert(make_pair(indexName, ptr));
    }

    index::IndexReaderPtr getIndexReader(const std::string &indexName) {
        return _indexReaders[indexName];
    }

    index::KeyIteratorPtr CreateKeyIterator(const std::string& indexName) override
    { assert(false); return index::KeyIteratorPtr(); };
    
    bool GenFieldMapMask(const std::string &indexName,
                         const std::vector<std::string>& termFieldNames,
                         fieldmap_t &targetFieldMap) override
    {
        if (indexName.find("expack") == std::string::npos) {
            return false;
        }
        targetFieldMap = 0;
        for (size_t i = 0; i < termFieldNames.size(); ++i) {
            int32_t fieldIdxInPack = autil::StringUtil::fromString<int32_t>(
                    termFieldNames[i].substr(5));
            targetFieldMap |= (fieldmap_t)(1 << fieldIdxInPack);
        }
        return true;
    }

private:
    std::map<std::string, index::SectionAttributeReaderPtr> _sectionReaders;
    std::map<std::string, index::IndexReaderPtr> _indexReaders;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeIndexReader);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_INDEX_READER_H
