#pragma once

#include <map>
#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/test/posting_maker.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class PackPostingMaker : public PostingMaker
{
public:
    PackPostingMaker();
    ~PackPostingMaker();

public:
    static void MakePostingData(PostingWriterImpl& writer, const DocMap& docMap);

    static void MakePostingDataWithLocalId(PostingWriterImpl& writer, const DocMap& docMap, docid_t baseDocId);

protected:
    static void UpdateSegmentInfo(index_base::SegmentInfo& segmentInfo, const std::string& sectionStr,
                                  docid_t baseDocId);
    static uint32_t GetDocCount(const std::string& sectionStr);

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<PackPostingMaker> PackPostingMakerPtr;
}}} // namespace indexlib::index::legacy
