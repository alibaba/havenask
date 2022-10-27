#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_position_maker.h"

using namespace std;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackPositionMaker);

void PackPositionMaker::MakeOneSegmentWithWriter(
        PostingWriterImpl& writer, 
        const PostingMaker::PostingList& postList, 
        optionflag_t optionFlag)
{
    const PostingMaker::DocRecordVector& docRecordVector = postList.second;

    for (PostingMaker::DocRecordVector::const_iterator it = docRecordVector.begin();
         it != docRecordVector.end(); ++it)
    {
        const PostingMaker::DocRecord& docRecord = *it;
        for (PostingMaker::DocRecord::const_iterator it2 = docRecord.begin();
             it2 != docRecord.end(); ++it2)
        {
            for (PostingMaker::PosValue::const_iterator it3 = it2->second.begin(); 
                 it3 != it2->second.end(); ++it3)
            {
                writer.AddPosition(it3->first, it3->second, 0);
            }
            writer.EndDocument(it2->first.first, it2->first.second);
        }
    }
}

IE_NAMESPACE_END(index);

