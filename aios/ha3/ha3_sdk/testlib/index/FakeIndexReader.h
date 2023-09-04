#pragma once

#include <assert.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3_sdk/testlib/index/FakePostingIterator.h"
#include "ha3_sdk/testlib/index/FakeTopIndexReader.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib {
namespace index {
class SectionAttributeReader;

class FakeIndexReader : public FakeTopIndexReader {
public:
    FakeIndexReader() {}
    ~FakeIndexReader() {}

public:
    /**
     * Get section reader
     */
    const SectionAttributeReader *GetSectionReader(const std::string &indexName) const {
        std::map<std::string, SectionAttributeReaderPtr>::const_iterator it
            = _sectionReaders.find(indexName);
        if (_sectionReaders.end() == it) {
            return NULL;
        }
        return it->second.get();
    }

    void addSectionReader(const std::string &indexName, SectionAttributeReaderPtr readerPtr) {
        _sectionReaders.insert(make_pair(indexName, readerPtr));
    }

    float GetCacheHitRatio(const std::string &indexName) const {
        return 0.0;
    }

    virtual index::Result<PostingIterator *> Lookup(const Term &term,
                                                    uint32_t statePoolSize = 1000,
                                                    PostingType type = pt_default,
                                                    autil::mem_pool::Pool *pool = NULL) {
        if (type == pt_bitmap) {
            std::string indexName = "bitmap_" + term.GetIndexName();
            std::map<std::string, std::shared_ptr<InvertedIndexReader>>::iterator it
                = _indexReaders.find(indexName);
            if (it == _indexReaders.end()) {
                return NULL;
            }
            return it->second->Lookup(term, statePoolSize, type, pool);
        }
        PostingIterator *iter = NULL;
        std::map<std::string, std::shared_ptr<InvertedIndexReader>>::iterator it
            = _indexReaders.find(term.GetTruncateName());
        if (it != _indexReaders.end()) {
            auto iterResult = it->second->Lookup(term, statePoolSize, type, pool);
            if (!iterResult.Ok()) {
                return iterResult.GetErrorCode();
            }
            iter = iterResult.Value();
        }
        PostingIterator *mainChainIter = NULL;
        it = _indexReaders.find(term.GetIndexName());
        if (it != _indexReaders.end()) {
            auto mainIterWithEc = it->second->Lookup(term, statePoolSize, type, pool);
            if (!mainIterWithEc.Ok()) {
                return mainIterWithEc.GetErrorCode();
            }
            mainChainIter = mainIterWithEc.Value();
        }
        PostingIterator *retIter = mainChainIter;
        if (iter) {
            FakePostingIterator *fakeIter = dynamic_cast<FakePostingIterator *>(iter);
            if (fakeIter && mainChainIter) {
                fakeIter->setTermMeta(mainChainIter->GetTermMeta());
            }
            POOL_COMPATIBLE_DELETE_CLASS(pool, mainChainIter);
            retIter = iter;
        }
        FakePostingIterator *fakeIter = dynamic_cast<FakePostingIterator *>(retIter);
        if (fakeIter && term.GetIndexName().find("expack_") != std::string::npos) {
            fakeIter->setType(pi_buffered);
        }
        return retIter;
    }

    void addIndexReader(const std::string &indexName, InvertedIndexReader *reader) {
        std::shared_ptr<InvertedIndexReader> ptr(reader);
        _indexReaders.insert(make_pair(indexName, ptr));
    }

    std::shared_ptr<InvertedIndexReader> getIndexReader(const std::string &indexName) {
        return _indexReaders[indexName];
    }

    virtual std::shared_ptr<indexlib::index::KeyIterator>
    CreateKeyIterator(const std::string &indexName) {
        assert(false);
        return nullptr;
    };
    /* override */
    bool GenFieldMapMask(const std::string &indexName,
                         const std::vector<std::string> &termFieldNames,
                         fieldmap_t &targetFieldMap) {
        if (indexName.find("expack") == std::string::npos) {
            return false;
        }
        targetFieldMap = 0;
        for (size_t i = 0; i < termFieldNames.size(); ++i) {
            int32_t fieldIdxInPack
                = autil::StringUtil::fromString<int32_t>(termFieldNames[i].substr(5));
            targetFieldMap |= (fieldmap_t)(1 << fieldIdxInPack);
        }
        return true;
    }

private:
    std::map<std::string, SectionAttributeReaderPtr> _sectionReaders;
    std::map<std::string, std::shared_ptr<InvertedIndexReader>> _indexReaders;
};

typedef std::shared_ptr<FakeIndexReader> FakeIndexReaderPtr;

class ExceptionIndexReader : public FakeIndexReader {
public:
    virtual index::Result<PostingIterator *> Lookup(const Term &term,
                                                    uint32_t statePoolSize = 1000,
                                                    PostingType type = pt_default,
                                                    autil::mem_pool::Pool *pool = NULL) {
        return indexlib::index::ErrorCode::FileIO;
    }
};

typedef std::shared_ptr<ExceptionIndexReader> ExceptionIndexReaderPtr;

} // namespace index
} // namespace indexlib
