#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_text_index_reader.h"

namespace indexlib { namespace testlib {

class FakeIndexReader : public index::LegacyIndexReader
{
public:
    FakeIndexReader() {}
    ~FakeIndexReader() {}

public:
    virtual std::string GetIndexName() const
    {
        assert(false);
        return "";
    }
    /**
     * Get section reader
     */
    const index::SectionAttributeReader* GetSectionReader(const std::string& indexName) const override
    {
        std::map<std::string, index::SectionAttributeReaderPtr>::const_iterator it = _sectionReaders.find(indexName);
        if (_sectionReaders.end() == it) {
            return NULL;
        }
        return it->second.get();
    }

    void addSectionReader(const std::string& indexName, index::SectionAttributeReaderPtr readerPtr)
    {
        _sectionReaders.insert(make_pair(indexName, readerPtr));
    }

    float GetCacheHitRatio(const std::string& indexName) const { return 0.0; }

    index::Result<index::PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                                  PostingType type = pt_default,
                                                  autil::mem_pool::Pool* pool = NULL) override
    {
        if (type == pt_bitmap) {
            std::string indexName = "bitmap_" + term.GetIndexName();
            std::map<std::string, std::shared_ptr<index::InvertedIndexReader>>::iterator it =
                _indexReaders.find(indexName);
            if (it == _indexReaders.end()) {
                return NULL;
            }
            return it->second->Lookup(term, statePoolSize, type, pool);
        }
        index::PostingIterator* iter = NULL;
        std::map<std::string, std::shared_ptr<index::InvertedIndexReader>>::iterator it =
            _indexReaders.find(term.GetTruncateName());
        if (it != _indexReaders.end()) {
            auto ret = it->second->Lookup(term, statePoolSize, type, pool);
            if (!ret.Ok()) {
                return ret.GetErrorCode();
            }
            iter = ret.Value();
        }
        index::PostingIterator* mainChainIter = NULL;
        it = _indexReaders.find(term.GetIndexName());
        if (it != _indexReaders.end()) {
            auto ret = it->second->Lookup(term, statePoolSize, type, pool);
            if (!ret.Ok()) {
                return ret.GetErrorCode();
            }
            mainChainIter = ret.Value();
        }
        index::PostingIterator* retIter = mainChainIter;
        if (iter) {
            FakePostingIterator* fakeIter = dynamic_cast<FakePostingIterator*>(iter);
            if (fakeIter && mainChainIter) {
                fakeIter->setTermMeta(mainChainIter->GetTermMeta());
            }
            POOL_COMPATIBLE_DELETE_CLASS(pool, mainChainIter);
            retIter = iter;
        }
        FakePostingIterator* fakeIter = dynamic_cast<FakePostingIterator*>(retIter);
        if (fakeIter && term.GetIndexName().find("expack_") != std::string::npos) {
            fakeIter->setType(index::pi_buffered);
        }
        return retIter;
    }

    future_lite::coro::Lazy<indexlib::index::Result<indexlib::index::PostingIterator*>>
    LookupAsync(const indexlib::index::Term* term, uint32_t statePoolSize, PostingType type,
                autil::mem_pool::Pool* pool, indexlib::file_system::ReadOption option) noexcept override
    {
        co_return Lookup(*term, statePoolSize, type, pool);
    }

    void addIndexReader(const std::string& indexName, index::InvertedIndexReader* reader)
    {
        std::shared_ptr<index::InvertedIndexReader> ptr(reader);
        _indexReaders.insert(make_pair(indexName, ptr));
    }

    std::shared_ptr<index::InvertedIndexReader> getIndexReader(const std::string& indexName)
    {
        return _indexReaders[indexName];
    }

    std::shared_ptr<index::KeyIterator> CreateKeyIterator(const std::string& indexName) override
    {
        assert(false);
        return std::shared_ptr<index::KeyIterator>();
    }

    bool GenFieldMapMask(const std::string& indexName, const std::vector<std::string>& termFieldNames,
                         fieldmap_t& targetFieldMap) override
    {
        if (indexName.find("expack") == std::string::npos) {
            return false;
        }
        targetFieldMap = 0;
        for (size_t i = 0; i < termFieldNames.size(); ++i) {
            int32_t fieldIdxInPack = autil::StringUtil::fromString<int32_t>(termFieldNames[i].substr(5));
            targetFieldMap |= (fieldmap_t)(1 << fieldIdxInPack);
        }
        return true;
    }

private:
    std::map<std::string, std::shared_ptr<index::SectionAttributeReader>> _sectionReaders;
    std::map<std::string, std::shared_ptr<index::InvertedIndexReader>> _indexReaders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeIndexReader);
}} // namespace indexlib::testlib
