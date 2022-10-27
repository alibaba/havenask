#ifndef ISEARCH_SUBTERMQUERYEXECUTOR_H
#define ISEARCH_SUBTERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h>
#include <ha3/search/TermQueryExecutor.h>

BEGIN_HA3_NAMESPACE(search);

class SubTermQueryExecutor : public TermQueryExecutor
{
public:
    typedef IE_NAMESPACE(index)::JoinDocidAttributeIterator DocMapAttrIterator;
public:
    SubTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter,
                         const common::Term &term,
                         DocMapAttrIterator *mainToSubIter,
                         DocMapAttrIterator *subToMainIter);
    ~SubTermQueryExecutor();
private:
    SubTermQueryExecutor(const SubTermQueryExecutor &);
    SubTermQueryExecutor& operator=(const SubTermQueryExecutor &);
public:
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t docId, docid_t& result) override;
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    bool isMainDocHit(docid_t docId) const override;
    void reset() override;
    IE_NAMESPACE(common)::ErrorCode unpackMatchData(rank::TermMatchData &tmd) override {
        // do nothing
        return IE_OK;
    }
public:
    std::string toString() const override;
private:
    inline IE_NAMESPACE(common)::ErrorCode subDocSeek(docid_t subDocId, docid_t& result) {
        if (subDocId > _curSubDocId) {
            auto ec = TermQueryExecutor::doSeek(subDocId, _curSubDocId);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
        result = _curSubDocId;
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
private:
    docid_t _curSubDocId;
    DocMapAttrIterator *_mainToSubIter;
    DocMapAttrIterator *_subToMainIter;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SubTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SUBTERMQUERYEXECUTOR_H
