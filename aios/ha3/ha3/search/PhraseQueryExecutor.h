#ifndef ISEARCH_SEARCH_PHRASESCORER_H_
#define ISEARCH_SEARCH_PHRASESCORER_H_

#include <ha3/common.h>
#include <ha3/search/AndQueryExecutor.h>
#include <ha3/search/TermQueryExecutor.h>
#include <ha3/util/Log.h>
#include <indexlib/index/normal/inverted_index/accessor/posting_iterator.h>

BEGIN_HA3_NAMESPACE(search);
class PhraseQueryExecutor : public AndQueryExecutor 
{
public:    
    PhraseQueryExecutor();
    ~PhraseQueryExecutor() override;

public:
    const std::string getName() const override {
        return "PhraseQueryExecutor";
    }
    IE_NAMESPACE(common)::ErrorCode doSeek(docid_t id, docid_t& result) override;
    IE_NAMESPACE(common)::ErrorCode seekSubDoc(docid_t docId, docid_t subDocId,
                       docid_t subDocEnd, bool needSubMatchdata, docid_t& result) override;
    void addTermQueryExecutors(const std::vector<TermQueryExecutor*> &termQueryExecutors);
    void addRelativePostion(int32_t termPos, int32_t postingMark);
public:
    std::string toString() const override;
protected:
    inline IE_NAMESPACE(common)::ErrorCode phraseFreq(bool& result);
protected:
    docid_t _lastMatchedDocId;
private:
    std::vector<TermQueryExecutor*> _termExecutors;
    std::vector<int32_t> _termReleativePos;
    std::vector<int32_t> _postingsMark;
    
private:
    HA3_LOG_DECLARE();
};


inline IE_NAMESPACE(common)::ErrorCode PhraseQueryExecutor::phraseFreq(bool& result) {
    int size = (int)_termReleativePos.size();
    int index = 0;
    int count = 0;//match count
    pos_t curPos = 0;
    if ( unlikely(0 == size) ) {
        result = true;
        return IE_OK;
    }
    do {
        int32_t relativePos = _termReleativePos[index];
        pos_t tmpocc = INVALID_POSITION;
        auto ec = _termExecutors[_postingsMark[index]]->seekPosition(
            curPos + relativePos, tmpocc);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (tmpocc == INVALID_POSITION) {
            result = false;
            return IE_OK;
        }
        if (tmpocc != curPos + relativePos) {
            curPos = tmpocc - relativePos;
            count = 1;
        } else {
            count ++;
        }
        ++index;
        if ( index == size ) {
            index = 0;
        }
    } while (count < size);

    result = true;
    return IE_OK;
} 

END_HA3_NAMESPACE(search);

#endif
