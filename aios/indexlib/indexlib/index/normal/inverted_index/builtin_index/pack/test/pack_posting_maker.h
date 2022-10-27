#ifndef __INDEXLIB_PACK_POSTING_MAKER_H
#define __INDEXLIB_PACK_POSTING_MAKER_H

#include <map>
#include <vector>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/test/posting_maker.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"

IE_NAMESPACE_BEGIN(index);

class PackPostingMaker : public PostingMaker
{
public:
    PackPostingMaker();
    ~PackPostingMaker();

public:
    static void MakePostingData(
            index::PostingWriterImpl& writer,
            const DocMap& docMap);

    static void MakePostingDataWithLocalId(
            index::PostingWriterImpl& writer,
            const DocMap& docMap, docid_t baseDocId);

protected:
    static void UpdateSegmentInfo(index_base::SegmentInfo& segmentInfo, 
                                  const std::string& sectionStr, 
                                  docid_t baseDocId);
    static uint32_t GetDocCount(const std::string& sectionStr);

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<PackPostingMaker> PackPostingMakerPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACK_POSTING_MAKER_H
