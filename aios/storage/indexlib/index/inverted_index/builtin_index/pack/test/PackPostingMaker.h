#pragma once

#include <map>
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/test/PostingMaker.h"

namespace indexlib::index {

class PackPostingMaker : public PostingMaker
{
public:
    PackPostingMaker() = default;
    ~PackPostingMaker() = default;

    static void MakePostingData(PostingWriterImpl& writer, const DocMap& docMap);
    static void MakePostingDataWithLocalId(PostingWriterImpl& writer, const DocMap& docMap, docid_t baseDocId);

protected:
    static void UpdateSegmentInfo(indexlibv2::framework::SegmentInfo& segmentInfo, const std::string& sectionStr,
                                  docid_t baseDocId);
    static uint32_t GetDocCount(const std::string& sectionStr);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
