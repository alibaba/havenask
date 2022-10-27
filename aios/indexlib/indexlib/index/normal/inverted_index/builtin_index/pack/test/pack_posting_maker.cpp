#include "indexlib/index/normal/inverted_index/builtin_index/pack/test/pack_posting_maker.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <sstream>

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackPostingMaker);

PackPostingMaker::PackPostingMaker() 
{
}

PackPostingMaker::~PackPostingMaker() 
{
}

void PackPostingMaker::MakePostingData(PostingWriterImpl& writer,
                           const PackPostingMaker::DocMap& docMap)
{
    for (PackPostingMaker::DocMap::const_iterator it = docMap.begin();
         it != docMap.end(); ++it)
    {
        for (PosValue::const_iterator it2 = it->second.begin(); 
             it2 != it->second.end(); ++it2)
        {
            writer.AddPosition(it2->first, it2->second, 0);
        }
        writer.EndDocument(it->first.first, it->first.second);
    }
}

void PackPostingMaker::MakePostingDataWithLocalId(PostingWriterImpl& writer,
            const DocMap& docMap, docid_t baseDocId)
{
    for (PackPostingMaker::DocMap::const_iterator it = docMap.begin();
         it != docMap.end(); ++it)
    {
        for (PosValue::const_iterator it2 = it->second.begin(); 
             it2 != it->second.end(); ++it2)
        {
            writer.AddPosition(it2->first, it2->second, 0);
        }
        writer.EndDocument(it->first.first - baseDocId, it->first.second);
    }    
}

uint32_t PackPostingMaker::GetDocCount(const string& sectionStr)
{
    autil::StringTokenizer st(sectionStr, ";", autil::StringTokenizer::TOKEN_TRIM |
                       autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    uint32_t docCount = 0;
    for (autil::StringTokenizer::Iterator it = st.begin();
         it != st.end(); ++it, ++docCount);
    return docCount;
}

void PackPostingMaker::UpdateSegmentInfo(index_base::SegmentInfo& segmentInfo, 
                                         const string& sectionStr, docid_t baseDocId)
{
    autil::StringTokenizer st(sectionStr, ";", autil::StringTokenizer::TOKEN_TRIM |
                       autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    uint32_t docCount = 0;
    for (autil::StringTokenizer::Iterator it = st.begin();
         it != st.end(); ++it, ++docCount);

    segmentInfo.docCount = docCount;
}

IE_NAMESPACE_END(index);
