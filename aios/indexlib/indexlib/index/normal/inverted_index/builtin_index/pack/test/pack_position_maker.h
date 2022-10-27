#ifndef __INDEXLIB_PACK_POSITION_MAKER_H
#define __INDEXLIB_PACK_POSITION_MAKER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_posting_maker.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"

IE_NAMESPACE_BEGIN(index);

class PackPositionMaker : public PackPostingMaker
{
public:
    static void MakeOneSegmentWithWriter(
            index::PostingWriterImpl& writer, 
            const PostingMaker::PostingList& postList,
            optionflag_t optionFlag = OPTION_FLAG_ALL);
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<PackPositionMaker> PackPositionMakerPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_POSITION_MAKER_H
