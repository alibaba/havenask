#ifndef ISEARCH_SUBFIELDMAPTERMQUERYEXECUTOR_H
#define ISEARCH_SUBFIELDMAPTERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SubTermQueryExecutor.h>
#include <ha3/search/FieldMapTermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h>

BEGIN_HA3_NAMESPACE(search);

class SubFieldMapTermQueryExecutor : public SubTermQueryExecutor
{
public:
    SubFieldMapTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter, 
                                 const common::Term &term,
                                 DocMapAttrIterator *mainToSubIter,
                                 DocMapAttrIterator *subToMainIter,
                                 fieldmap_t fieldMap,
                                 FieldMatchOperatorType opteratorType);
                                 
    ~SubFieldMapTermQueryExecutor();
private:
    SubFieldMapTermQueryExecutor(const SubFieldMapTermQueryExecutor &);
    SubFieldMapTermQueryExecutor& operator=(const SubFieldMapTermQueryExecutor &);
public:
    const std::string getName() const override {
        return "SubFieldMapTermQueryExecutor";
    }
    std::string toString() const override;
    void reset() override {
        SubTermQueryExecutor::reset();
        initBufferedPostingIterator();
    }

private:
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(
        docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
private:
    void initBufferedPostingIterator();
private:
    IE_NAMESPACE(index)::BufferedPostingIterator *_bufferedIter;
    fieldmap_t _fieldMap;
    FieldMatchOperatorType _opteratorType;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SubFieldMapTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SUBFIELDMAPTERMQUERYEXECUTOR_H
