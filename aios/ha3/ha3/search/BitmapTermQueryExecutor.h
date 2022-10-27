#ifndef ISEARCH_BITMAPTERMQUERYEXECUTOR_H
#define ISEARCH_BITMAPTERMQUERYEXECUTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/TermQueryExecutor.h>
#include <indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h>

BEGIN_HA3_NAMESPACE(search);

class BitmapTermQueryExecutor : public TermQueryExecutor
{
public:
    BitmapTermQueryExecutor(IE_NAMESPACE(index)::PostingIterator *iter, 
                            const common::Term &term);
    ~BitmapTermQueryExecutor();
private:
    BitmapTermQueryExecutor(const BitmapTermQueryExecutor &);
    BitmapTermQueryExecutor& operator=(const BitmapTermQueryExecutor &);
public:
    void reset() override;
    const std::string getName() const override {
        return "BitmapTermQueryExecutor";
    }
public:
    inline bool test(docid_t docId) {
        if (_bitmapIter->Test(docId)) {
            setDocId(docId);
            return true;
        }
        return false;
    }
private:
    void initBitmapIterator() {
        _bitmapIter = dynamic_cast<IE_NAMESPACE(index)::BitmapPostingIterator*>(_iter);
    }
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override {
        docid_t tempDocId = INVALID_DOCID;
        auto ec = _bitmapIter->InnerSeekDoc(id, tempDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        ++_seekDocCount;
        if (tempDocId == INVALID_DOCID) {
            tempDocId = END_DOCID;
        }
        result = tempDocId;
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
private:
    IE_NAMESPACE(index)::BitmapPostingIterator *_bitmapIter;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(BitmapTermQueryExecutor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_BITMAPTERMQUERYEXECUTOR_H
