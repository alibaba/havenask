#include "indexlib/index/inverted_index/builtin_index/pack/test/PackPositionMaker.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PackPositionMaker);

void PackPositionMaker::MakeOneSegmentWithWriter(PostingWriterImpl& writer, const PostingMaker::PostingList& postList,
                                                 optionflag_t optionFlag)
{
    const PostingMaker::DocRecordVector& docRecordVector = postList.second;

    for (PostingMaker::DocRecordVector::const_iterator it = docRecordVector.begin(); it != docRecordVector.end();
         ++it) {
        const PostingMaker::DocRecord& docRecord = *it;
        for (PostingMaker::DocRecord::const_iterator it2 = docRecord.begin(); it2 != docRecord.end(); ++it2) {
            for (PostingMaker::PosValue::const_iterator it3 = it2->second.begin(); it3 != it2->second.end(); ++it3) {
                writer.AddPosition(it3->first, it3->second, 0);
            }
            writer.EndDocument(it2->first.first, it2->first.second);
        }
    }
}
} // namespace indexlib::index
