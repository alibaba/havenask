#ifndef __INDEXLIB_EXPACK_POSTING_MAKER_H
#define __INDEXLIB_EXPACK_POSTING_MAKER_H

#include <tr1/memory>
#include <vector>
#include <map>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index/test/posting_maker.h"

IE_NAMESPACE_BEGIN(index);

class ExpackPostingMaker
{
public:
    typedef std::pair<docid_t, docpayload_t> DocKey;
    typedef std::map<DocKey, fieldmap_t> ExpackDocMapWithFieldMap;
    typedef ExpackDocMapWithFieldMap ExpackDocRecord;
    typedef std::tr1::shared_ptr<ExpackDocRecord> ExpackDocRecordPtr;
    typedef std::vector<ExpackDocRecord> ExpackDocRecordVector;
    typedef std::pair<docid_t, ExpackDocRecordVector> ExpackPostingList; // <newBaseId, Posting>

    struct DocInfo
    {
        DocInfo(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap)
        {
            mNewDocId = docId;
            mDocPayload = docPayload;
            mFieldMap = fieldMap;
        }

        docid_t mNewDocId;
        docpayload_t mDocPayload;
        fieldmap_t mFieldMap;
    };

    struct DocInfoComparator
    {
        bool operator() (const DocInfo& left, const DocInfo& right)
        {
            return left.mNewDocId < right.mNewDocId;
        }
    };  
   
public:
    ExpackPostingMaker();
    virtual ~ExpackPostingMaker();

public:
    static void MakeOneSegmentWithWriter(
            index::PostingWriterImpl& writer, 
            const PostingMaker::PostingList& postList, 
            ExpackPostingList& expackPostList);

    static ExpackPostingList MergeExpackPostingLists(
            std::vector<ExpackPostingList>& postLists);

    static void AddToMerge(ExpackPostingList& mergedList, 
                           const ExpackPostingList& postList,
                           bool toAlignBuffer = true);

    static void CreateReclaimedPostingList(ExpackPostingList& destPostingList,
            const ExpackPostingList& srcPostingList, const ReclaimMap& reclaimMap);

    static ExpackPostingList SortByWeightMergePostingLists(
            std::vector<ExpackPostingList>& postLists, const ReclaimMap& reclaimMap);

    static void BuildPostingData(
            index::PostingWriterImpl& writer,
            const PostingMaker::DocMap& docMap,
            docid_t baseDocId);
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<ExpackPostingMaker> ExpackPostingMakerPtr;


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_EXPACK_POSTING_MAKER_H
