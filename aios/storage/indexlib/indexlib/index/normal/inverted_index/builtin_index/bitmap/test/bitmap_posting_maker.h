#ifndef __INDEXLIB_BITMAP_POSTING_MAKER_H
#define __INDEXLIB_BITMAP_POSTING_MAKER_H

#include <map>
#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/test/posting_maker.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

typedef uint32_t (*TokenNumFunc)();

class BitmapPostingMaker : public PostingMaker
{
public:
    typedef std::map<std::string, std::vector<docid_t>> Key2DocIdListMap;
    typedef std::pair<docid_t, std::vector<docid_t>> PostingList; // <baseId, Posting>

    BitmapPostingMaker(const indexlib::file_system::DirectoryPtr& rootDir, const std::string& indexName,
                       bool enableNullTerm);
    ~BitmapPostingMaker();

public:
    void MakeOnePostingList(segmentid_t segId, uint32_t docNum, docid_t baseDocId, PostingList* answer);

    void MakeOneSegmentData(segmentid_t segId, uint32_t docNum, docid_t baseDocId, Key2DocIdListMap* answer);

    void MakeMultiSegmentsData(const std::vector<uint32_t>& docNums, Key2DocIdListMap* answer);

private:
    static uint32_t RandomTokenNum();
    static uint32_t OneTokenNum();

    void InnerMakeOneSegmentData(segmentid_t segId, uint32_t docNum, docid_t baseDocId, Key2DocIdListMap* answer,
                                 TokenNumFunc tokenNumFunc);

    indexlib::file_system::DirectoryPtr mRootDir;
    std::string mIndexName;
    bool mEnableNullTerm;
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<BitmapPostingMaker> BitmapPostingMakerPtr;
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_BITMAP_POSTING_MAKER_H
