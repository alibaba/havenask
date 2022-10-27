#include <autil/StringUtil.h>
#include "indexlib/index/normal/inverted_index/test/posting_writer_helper.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingWriterHelper);

PostingWriterHelper::PostingWriterHelper() 
{
}

PostingWriterHelper::~PostingWriterHelper() 
{
}

void PostingWriterHelper::CreateAnswer(
        uint32_t docCount, Answer& answer)
{
    tf_t totalTF = 0;
    for (uint32_t i = 0; i < docCount; i++)
    {
        tf_t tf = rand() % 10 + 1;
        docpayload_t docPayload = i % 10;

        AnswerInDoc answerInDoc;
        answerInDoc.docPayload = docPayload;

        //generate pos and pospayload
        for (tf_t j = 0; j < tf; j++)
        {
            pos_t pos = i + j;
            pospayload_t payload = (i + j) % 10;
            int32_t fieldIdxInPack = MockFieldIdx(tf, j);
            
            answerInDoc.fieldMap |= (1 << fieldIdxInPack);
            answerInDoc.positionList.push_back(pos);
            answerInDoc.fieldIdxList.push_back(fieldIdxInPack);
            answerInDoc.posPayloadList.push_back(payload);
            
            totalTF++;
        }

        answer.inDocAnswers.insert(make_pair(i, answerInDoc));
        answer.docIdList.push_back(i);
        answer.tfList.push_back(tf);
    }
    answer.totalTF = totalTF;
}

void PostingWriterHelper::Build(PostingWriterImplPtr& writer, Answer& answer)
{
    for (uint32_t i = 0; i < answer.docIdList.size(); ++i)
    {
        AnswerInDoc& answerInDoc = answer.inDocAnswers[answer.docIdList[i]];
        for (uint32_t j = 0; j < answerInDoc.positionList.size(); ++j)
        {
            pos_t pos = answerInDoc.positionList[j];
            uint32_t fieldIdx = answerInDoc.fieldIdxList[j];
            pospayload_t posPayload = answerInDoc.posPayloadList[j];
            writer->AddPosition(pos, posPayload, fieldIdx);
        }
        writer->EndDocument(i, answerInDoc.docPayload);
    }
}

int32_t PostingWriterHelper::MockFieldIdx(tf_t tf, tf_t idx)
{
    int32_t maxFieldCount = 8;
    int32_t fieldCount = (rand() % maxFieldCount) + 1;
    //for generate fieldIdx
    int32_t leftPosCount = tf % fieldCount;
    int32_t avgPosCount = tf / fieldCount;
    //pos in field1 must smaller than pos in field2
    return  idx < leftPosCount ? 0 : (idx - leftPosCount) / avgPosCount;
}

IE_NAMESPACE_END(index);

