#pragma once

#include <memory>

#include "autil/Log.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/builtin_index/pack/test/PackPostingMaker.h"

namespace indexlib::index {

class PackPositionMaker : public PackPostingMaker
{
public:
    static void MakeOneSegmentWithWriter(PostingWriterImpl& writer, const PostingMaker::PostingList& postList,
                                         optionflag_t optionFlag = OPTION_FLAG_ALL);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
