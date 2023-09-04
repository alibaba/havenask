#ifndef __INDEXLIB_POSTING_MAKER_H
#define __INDEXLIB_POSTING_MAKER_H

#include <map>
#include <memory>
#include <vector>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class PostingMaker
{
public:
    typedef std::pair<docid_t, docpayload_t> DocKey;
    typedef std::vector<std::pair<pos_t, pospayload_t>> PosValue;
    typedef std::map<DocKey, PosValue> DocMap;
    typedef DocMap DocRecord;
    typedef std::shared_ptr<DocRecord> DocRecordPtr;
    typedef std::vector<DocRecord> DocRecordVector;
    typedef std::pair<docid_t, DocRecordVector> PostingList; // <newBaseId, Posting>
    typedef std::vector<std::vector<fieldid_t>> FieldIdVec;
    typedef std::shared_ptr<FieldIdVec> FieldIdVecPtr;

public:
    struct Buffer {
        Buffer()
            : mDocBase(0)
            , mTfDocPayloadBase(0)
            , mPosBase(0)
            , mCurDocId(0)
            , mCursorInPosBuffer(0)
            , mPosReadCount(0)
            , mAccessPosCount(0)
        {
        }

        uint32_t mDocBase;
        uint32_t mTfDocPayloadBase;
        uint32_t mPosBase;

        docid_t mCurDocId;

        docid_t mDocBuffer[MAX_DOC_PER_RECORD];
        docpayload_t mDocPayloadBuffer[MAX_DOC_PER_RECORD];
        tf_t mTfBuffer[MAX_DOC_PER_RECORD];

        pos_t mPosBuffer[MAX_DOC_PER_RECORD];
        pospayload_t mPosPayloadBuffer[MAX_DOC_PER_RECORD];
        uint32_t mCursorInPosBuffer;
        uint32_t mPosReadCount;
        uint32_t mAccessPosCount;
    };

    struct DocInfo {
        DocInfo(docid_t docId, docpayload_t docPayload, PosValue& posValue)
        {
            mNewDocId = docId;
            mDocPayload = docPayload;
            mPosValue = posValue;
        }

        docid_t mNewDocId;
        docpayload_t mDocPayload;
        PosValue mPosValue;
    };

    struct DocInfoComparator {
        bool operator()(const DocInfo& left, const DocInfo& right) { return left.mNewDocId < right.mNewDocId; }
    };

public:
    PostingMaker();
    ~PostingMaker();

public:
    /**
     * make doc map from string
     * format: "docid docPayload, (pos posPayload, pos posPayload, ...); ....
     */
    static DocMap MakeDocMap(const std::string& str);

    /*
     * build posting list using given docMap
     */
    template <typename WriterType>
    static void BuildPostingData(WriterType& writer, const DocMap& docMap, docid_t baseDocId);

    static PostingList MakePostingData(uint32_t docCount, docid_t& maxDocId, tf_t& totalTf);

    static PostingList MakePostingData(const std::vector<uint32_t>& numDocsInRecords, docid_t& maxDocId, tf_t& totalTf);

    static PostingList MakePostingList(const std::vector<std::string>& strs);

    static PostingList MergePostingLists(std::vector<PostingList>& postLists);

    static PostingList SortByWeightMergePostingLists(std::vector<PostingList>& postLists, const ReclaimMap& reclaimMap);

    static void AddToMerge(PostingList& mergedList, const PostingList& postList, bool toAlignBuffer = true);

    static void CreateReclaimedPostingList(PostingList& destPostingList, const PostingList& srcPostingList,
                                           const ReclaimMap& reclaimMap);

    static PostingList MakePositionData(uint32_t docCount, docid_t& maxDocId, uint32_t& totalTF);
    static PostingList MakePositionData(const std::vector<uint32_t>& numDocsInRecords, docid_t& maxDocId,
                                        uint32_t& totalTF);

private:
    static void ParseKeyValue(const std::string& str, const std::string& splite, std::string& key, std::string& value);

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<PostingMaker> PostingMakerPtr;

template <typename WriterType>
inline void PostingMaker::BuildPostingData(WriterType& writer, const DocMap& docMap, docid_t baseDocId)
{
    for (DocMap::const_iterator it = docMap.begin(); it != docMap.end(); ++it) {
        for (PosValue::const_iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            int32_t fieldIdxInPack = (it2->first) % 8;
            writer.AddPosition(it2->first, it2->second, fieldIdxInPack);
        }
        writer.EndDocument(it->first.first - baseDocId, it->first.second);
    }
}
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_POSTING_MAKER_H
