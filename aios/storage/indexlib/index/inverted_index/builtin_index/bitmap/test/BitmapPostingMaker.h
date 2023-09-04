#pragma once

#include "autil/Log.h"
#include "indexlib/index/inverted_index/test/PostingMaker.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::index {

typedef uint32_t (*TokenNumFunc)();

class BitmapPostingMaker : public PostingMaker
{
public:
    using Key2DocIdListMap = std::map<std::string, std::vector<docid_t>>;
    using PostingList = std::pair<docid_t, std::vector<docid_t>>; // <baseId, Posting>

    BitmapPostingMaker(const std::shared_ptr<file_system::Directory>& rootDir, const std::string& indexName,
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

    std::shared_ptr<file_system::Directory> _rootDir;
    std::string _indexName;
    bool _enableNullTerm;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
