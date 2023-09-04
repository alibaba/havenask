#include "indexlib/index/inverted_index/builtin_index/pack/test/PackPostingMaker.h"

#include <sstream>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PackPostingMaker);

void PackPostingMaker::MakePostingData(PostingWriterImpl& writer, const PackPostingMaker::DocMap& docMap)
{
    for (PackPostingMaker::DocMap::const_iterator it = docMap.begin(); it != docMap.end(); ++it) {
        for (PosValue::const_iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            writer.AddPosition(it2->first, it2->second, 0);
        }
        writer.EndDocument(it->first.first, it->first.second);
    }
}

void PackPostingMaker::MakePostingDataWithLocalId(PostingWriterImpl& writer, const DocMap& docMap, docid_t baseDocId)
{
    for (PackPostingMaker::DocMap::const_iterator it = docMap.begin(); it != docMap.end(); ++it) {
        for (PosValue::const_iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            writer.AddPosition(it2->first, it2->second, 0);
        }
        writer.EndDocument(it->first.first - baseDocId, it->first.second);
    }
}

uint32_t PackPostingMaker::GetDocCount(const std::string& sectionStr)
{
    autil::StringTokenizer st(sectionStr, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    uint32_t docCount = 0;
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it, ++docCount)
        ;
    return docCount;
}

void PackPostingMaker::UpdateSegmentInfo(indexlibv2::framework::SegmentInfo& segmentInfo, const std::string& sectionStr,
                                         docid_t baseDocId)
{
    autil::StringTokenizer st(sectionStr, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    uint32_t docCount = 0;
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it, ++docCount)
        ;

    segmentInfo.docCount = docCount;
}
} // namespace indexlib::index
