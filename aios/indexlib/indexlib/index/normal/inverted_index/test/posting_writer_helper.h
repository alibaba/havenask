#ifndef __INDEXLIB_POSTING_WRITER_HELPER_H
#define __INDEXLIB_POSTING_WRITER_HELPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/test/index_document_maker.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"

IE_NAMESPACE_BEGIN(index);

typedef index::IndexDocumentMaker::KeyAnswer Answer;
typedef index::IndexDocumentMaker::KeyAnswerInDoc AnswerInDoc;

class PostingWriterHelper
{
public:
    PostingWriterHelper();
    ~PostingWriterHelper();
public:
    //mock data and  store them to Answer
    static void CreateAnswer(
            uint32_t docCount, Answer& answer);

    static void Build(PostingWriterImplPtr& writer, Answer& answer);
    static int32_t MockFieldIdx(tf_t tf, tf_t idx);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingWriterHelper);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_WRITER_HELPER_H
