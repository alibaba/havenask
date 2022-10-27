#ifndef ISEARCH_FAKEINDEXREADER_H
#define ISEARCH_FAKEINDEXREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3_sdk/testlib/index/FakeTopIndexReader.h>
#include <ha3_sdk/testlib/index/FakePostingIterator.h>

IE_NAMESPACE_BEGIN(index);

class FakeIndexReader : public FakeTopIndexReader
{
public:
    FakeIndexReader() {}
    ~FakeIndexReader() {}
public:
    /**
     * Get section reader 
     */
    const SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const 
    {
        std::map<std::string, SectionAttributeReaderPtr>::const_iterator it
            = _sectionReaders.find(indexName);
        if (_sectionReaders.end() == it) {
            return NULL;
        }
        return it->second.get();
    }

    void addSectionReader(const std::string &indexName, 
                          SectionAttributeReaderPtr readerPtr) 
    {
        _sectionReaders.insert(make_pair(indexName, readerPtr));
    }

    float GetCacheHitRatio(const std::string& indexName) const { return 0.0; }

    virtual PostingIterator *Lookup(const common::Term& term, uint32_t statePoolSize = 1000,
                                    PostingType type = pt_default,
                                    autil::mem_pool::Pool *pool = NULL)
    {
        if (type == pt_bitmap) {
            std::string indexName = "bitmap_" + term.GetIndexName();
            std::map<std::string, IndexReaderPtr>::iterator it = _indexReaders.find(indexName);
            if (it == _indexReaders.end()) {
                return NULL;
            }
            return it->second->Lookup(term, statePoolSize, type, pool);
        }
        PostingIterator *iter = NULL;
        std::map<std::string, IndexReaderPtr>::iterator it = _indexReaders.find(term.GetTruncateName());
        if (it != _indexReaders.end()) {
            iter = it->second->Lookup(term, statePoolSize, type, pool);
        }
        PostingIterator *mainChainIter = NULL;
        it = _indexReaders.find(term.GetIndexName());
        if (it != _indexReaders.end()) {
            mainChainIter = it->second->Lookup(term, statePoolSize, type, pool);
        }
        PostingIterator *retIter = mainChainIter;
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

    void addIndexReader(const std::string &indexName, IndexReader *reader) {
        IndexReaderPtr ptr(reader);
        _indexReaders.insert(make_pair(indexName, ptr));
    }

    IndexReaderPtr getIndexReader(const std::string &indexName) {
        return _indexReaders[indexName];
    }

    virtual KeyIteratorPtr CreateKeyIterator(const std::string& indexName) { assert(false); return KeyIteratorPtr(); };
    /* override */
    bool GenFieldMapMask(const std::string &indexName,
                         const std::vector<std::string>& termFieldNames,
                         fieldmap_t &targetFieldMap)
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
    std::map<std::string, SectionAttributeReaderPtr> _sectionReaders;
    std::map<std::string, IndexReaderPtr> _indexReaders;
};

HA3_TYPEDEF_PTR(FakeIndexReader);

class ExceptionIndexReader : public FakeIndexReader
{
public:
    virtual PostingIterator *Lookup(const common::Term& term, uint32_t statePoolSize = 1000,
                                    PostingType type = pt_default,
                                    autil::mem_pool::Pool *pool = NULL) {
        AUTIL_LEGACY_THROW(misc::FileIOException, "fake exception");
    }
};

HA3_TYPEDEF_PTR(ExceptionIndexReader);

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEINDEXREADER_H
