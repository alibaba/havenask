#pragma once
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/PostingIterator.h"

namespace indexlibv2::table {

class Query
{
public:
    virtual ~Query() = default;
    virtual docid_t Seek(docid_t docid) = 0;
};

class DocidQuery : public Query
{
public:
    bool Init(const std::string& docidStr)
    {
        std::vector<docid_t> docidRange;
        autil::StringUtil::fromString(docidStr, docidRange, "-");
        if (docidRange.size() == 1) {
            _docIdVec.push_back(docidRange[0]);
            return true;
        }
        if (docidRange.size() == 2) {
            // [begin, end]
            for (docid_t docid = docidRange[0]; docid <= docidRange[1]; ++docid) {
                _docIdVec.push_back(docid);
            }
            return true;
        }
        return false;
    }
    docid_t Seek(docid_t docid) override
    {
        while (_cursor < _docIdVec.size()) {
            docid_t retDocId = _docIdVec[_cursor++];
            if (retDocId >= docid) {
                return retDocId;
            }
        }
        return INVALID_DOCID;
    }

private:
    std::vector<docid_t> _docIdVec;
    size_t _cursor;
};

class TermQuery : public Query
{
public:
    ~TermQuery() { indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _iter); }

    void Init(indexlib::index::PostingIterator* iter, autil::mem_pool::Pool* pool, std::string indexName)
    {
        _iter = iter;
        _pool = pool;
    }
    docid_t Seek(docid_t docid) override { return _iter->SeekDoc(docid); }

private:
    indexlib::index::PostingIterator* _iter = nullptr;
    autil::mem_pool::Pool* _pool = nullptr;
};

class PrimaryKeyQuery : public Query
{
public:
    void Init(docid_t docId)
    {
        _docId = docId;
        _isSearched = false;
    }
    docid_t Seek(docid_t docid) override
    {
        if (_isSearched) {
            return INVALID_DOCID;
        }
        _isSearched = true;
        return _docId;
    }

private:
    docid_t _docId;
    bool _isSearched;
};

} // namespace indexlibv2::table
