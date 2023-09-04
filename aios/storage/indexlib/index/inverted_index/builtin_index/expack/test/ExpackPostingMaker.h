#pragma once

#include "autil/Log.h"
#include "indexlib/index/inverted_index/test/PostingMaker.h"

namespace indexlibv2::index {
class DocMapper;
}

namespace indexlib::index {
class PostingWriterImpl;
class ExpackPostingMaker
{
public:
    typedef std::pair<docid_t, docpayload_t> DocKey;
    typedef std::map<DocKey, fieldmap_t> ExpackDocMapWithFieldMap;
    typedef ExpackDocMapWithFieldMap ExpackDocRecord;
    typedef std::shared_ptr<ExpackDocRecord> ExpackDocRecordPtr;
    typedef std::vector<ExpackDocRecord> ExpackDocRecordVector;
    typedef std::pair<docid_t, ExpackDocRecordVector> ExpackPostingList; // <newBaseId, Posting>

    struct DocInfo {
        DocInfo(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap)
        {
            _newDocId = docId;
            _docPayload = docPayload;
            _fieldMap = fieldMap;
        }

        docid_t _newDocId;
        docpayload_t _docPayload;
        fieldmap_t _fieldMap;
    };

    struct DocInfoComparator {
        bool operator()(const DocInfo& left, const DocInfo& right) { return left._newDocId < right._newDocId; }
    };

    ExpackPostingMaker() = default;
    virtual ~ExpackPostingMaker() = default;

    static void MakeOneSegmentWithWriter(PostingWriterImpl& writer, const PostingMaker::PostingList& postList,
                                         ExpackPostingList& expackPostList);

    static ExpackPostingList MergeExpackPostingLists(std::vector<ExpackPostingList>& postLists);

    static void AddToMerge(ExpackPostingList& mergedList, const ExpackPostingList& postList, bool toAlignBuffer = true);

    static void CreateReclaimedPostingList(ExpackPostingList& destPostingList, const ExpackPostingList& srcPostingList,
                                           const indexlibv2::index::DocMapper& docMapper);

    static ExpackPostingList SortByWeightMergePostingLists(std::vector<ExpackPostingList>& postLists,
                                                           const indexlibv2::index::DocMapper& docMapper);

    static void BuildPostingData(PostingWriterImpl& writer, const PostingMaker::DocMap& docMap, docid_t baseDocId);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
