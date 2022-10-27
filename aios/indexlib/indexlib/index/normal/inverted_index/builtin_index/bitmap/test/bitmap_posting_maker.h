#ifndef __INDEXLIB_BITMAP_POSTING_MAKER_H
#define __INDEXLIB_BITMAP_POSTING_MAKER_H

#include <map>
#include <vector>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/test/posting_maker.h"

IE_NAMESPACE_BEGIN(index);

typedef uint32_t (*TokenNumFunc)();

class BitmapPostingMaker : public PostingMaker
{
public:
    typedef std::map<std::string, std::vector<docid_t> > Key2DocIdListMap;
    typedef std::pair<docid_t, std::vector<docid_t> > PostingList; // <baseId, Posting>

    BitmapPostingMaker(const std::string& rootDir,
                       const std::string& indexName);
    ~BitmapPostingMaker();

public:
    void MakeOnePostingList(segmentid_t segId, uint32_t docNum,
                            docid_t baseDocId, PostingList& answer);

    void MakeOneSegmentData(segmentid_t segId, 
                            uint32_t docNum,
                            docid_t baseDocId, 
                            Key2DocIdListMap& answer);

    void MakeMultiSegmentsData(const std::vector<uint32_t>& docNums, 
                               Key2DocIdListMap& answer);
private:
    static uint32_t RandomTokenNum();
    static uint32_t OneTokenNum();

    void InnerMakeOneSegmentData(segmentid_t segId, 
                            uint32_t docNum,
                            docid_t baseDocId, 
                            Key2DocIdListMap& answer,
                            TokenNumFunc tokenNumFunc);

    std::string mRootDir;
    std::string mIndexName;

    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<BitmapPostingMaker> BitmapPostingMakerPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_POSTING_MAKER_H
