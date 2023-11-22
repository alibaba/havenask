#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_posting_maker.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class PackPositionMaker : public PackPostingMaker
{
public:
    static void MakeOneSegmentWithWriter(PostingWriterImpl& writer, const PostingMaker::PostingList& postList,
                                         optionflag_t optionFlag = OPTION_FLAG_ALL);

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<PackPositionMaker> PackPositionMakerPtr;
}}} // namespace indexlib::index::legacy
