#ifndef ISEARCH_MATCHDOCCREATOR_H
#define ISEARCH_MATCHDOCCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Ha3MatchDocAllocator.h>

BEGIN_HA3_NAMESPACE(common);

class SimpleDocIdIterator;

class MatchDocCreator
{
public:
    MatchDocCreator();
    ~MatchDocCreator();
private:
    MatchDocCreator(const MatchDocCreator &);
    MatchDocCreator &operator=(const MatchDocCreator &);
public:
    void createMatchDocs(const std::string &matchDocStr,
                         std::vector<matchdoc::MatchDoc> &matchDocs);
    void destroyMatchDocs(const std::vector<matchdoc::MatchDoc> &matchDocs);
public:
    common::Ha3MatchDocAllocator *getMatchDocAllocator() const {
        return _mdAllocator;
    }
    autil::mem_pool::Pool *getPool() const {
        return _pool;
    }
public:
    static void allocateMatchDocs(const std::string &docIdStr,
                                  std::vector<matchdoc::MatchDoc> &matchDocs,
                                  common::Ha3MatchDocAllocator *mdAllocator);
    static void createMatchDocs(const std::string &matchDocStr,
                                std::vector<matchdoc::MatchDoc> &matchDocs,
                                common::Ha3MatchDocAllocator *mdAllocator,
                                autil::mem_pool::Pool *pool);
    static void deallocateMatchDocs(const std::vector<matchdoc::MatchDoc> &matchDocs,
                                    common::Ha3MatchDocAllocator *mdAllocator);
    static void allocateMatchDocWithSubDoc(const std::string &docIdStr,
            std::vector<matchdoc::MatchDoc> &matchDocs,
            common::Ha3MatchDocAllocator *allocator);
    static SimpleDocIdIterator createDocIterator(const std::string &docStr);
private:
    static void createDocVector(const std::string &docStr,
                                std::vector<int> &docIds,
                                std::vector<int> &subDocIdNums,
                                std::vector<int> &subDocIds);
private:
    common::Ha3MatchDocAllocator *_mdAllocator;
    autil::mem_pool::Pool *_pool;
private:
    HA3_LOG_DECLARE();
};

class SimpleDocIdIterator {
public:
    // docId vector layout: [docid, subdocid_num, docid, subdocid_num ...]
    // subdocId vector layout: [subdocid, subdocid, subdocid ...]
    SimpleDocIdIterator(const std::vector<int> &docIds,
                        const std::vector<int> &subDocIdNums,
                        const std::vector<int> &subDocIds)
        : _docIds(docIds)
        , _subDocIdNums(subDocIdNums)
        , _subDocIds(subDocIds)
        , _curDocIdx(0)
        , _curSubDocEnd(0)
        , _curSubDocIdx(0)
    {
    }

    docid_t getCurDocId() {
        assert(_curDocIdx < _docIds.size());
        return _docIds[_curDocIdx];
    }
    bool beginSubDoc() {
        _curSubDocEnd = _curSubDocEnd + _subDocIdNums[_curDocIdx];
        return true;
    }
    docid_t nextSubDocId() {
        if (_curSubDocIdx < _curSubDocEnd) {
            return _subDocIds[_curSubDocIdx++];
        } else {
            return INVALID_DOCID;
        }
    }
    void next() {
        _curDocIdx++;
    }
    bool hasNext() {
        return _curDocIdx < _docIds.size();
    }
private:
    std::vector<int> _docIds;
    std::vector<int> _subDocIdNums;
    std::vector<int> _subDocIds;
    size_t _curDocIdx;
    size_t _curSubDocEnd;
    size_t _curSubDocIdx;
};

HA3_TYPEDEF_PTR(MatchDocCreator);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_MATCHDOCCREATOR_H
