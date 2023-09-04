#include "indexlib/index/inverted_index/builtin_index/expack/test/ExpackPostingMaker.h"

#include "indexlib/index/DocMapper.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/util/Random.h"

namespace indexlib::index {
namespace {
using indexlibv2::index::DocMapper;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, ExpackPostingMaker);

void ExpackPostingMaker::BuildPostingData(PostingWriterImpl& writer, const PostingMaker::DocMap& docMap,
                                          docid_t baseDocId)
{
    for (PostingMaker::DocMap::const_iterator it = docMap.begin(); it != docMap.end(); ++it) {
        for (PostingMaker::PosValue::const_iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
            int32_t fieldIdxInPack = (it2->first) % 8;
            writer.AddPosition(it2->first, it2->second, fieldIdxInPack);
        }
        writer.EndDocument(it->first.first - baseDocId, it->first.second);
    }
}

ExpackPostingMaker::ExpackPostingList
ExpackPostingMaker::MergeExpackPostingLists(std::vector<ExpackPostingMaker::ExpackPostingList>& expackPostLists)
{
    ExpackPostingList mergedList;
    for (std::vector<ExpackPostingList>::const_iterator it = expackPostLists.begin(); it != expackPostLists.end();
         ++it) {
        AddToMerge(mergedList, *it);
    }
    return mergedList;
}

void ExpackPostingMaker::MakeOneSegmentWithWriter(PostingWriterImpl& writer, const PostingMaker::PostingList& postList,
                                                  ExpackPostingList& expackPostingList)
{
    const PostingMaker::DocRecordVector& docRecordVector = postList.second;
    ExpackDocRecordVector expackDocRecordVector;
    for (PostingMaker::DocRecordVector::const_iterator recordIt = docRecordVector.begin();
         recordIt != docRecordVector.end(); ++recordIt) {
        const PostingMaker::DocRecord& docRecord = *recordIt;
        ExpackDocRecord expackDocRecord;
        for (PostingMaker::DocRecord::const_iterator docIt = docRecord.begin(); docIt != docRecord.end(); ++docIt) {
            fieldmap_t fieldMap = 0;
            for (PostingMaker::PosValue::const_iterator posIt = docIt->second.begin(); posIt != docIt->second.end();
                 ++posIt) {
                int32_t fieldIdxInPack = util::dev_urandom() % 8;
                writer.AddPosition(posIt->first, posIt->second, fieldIdxInPack);
                fieldMap |= (fieldmap_t)(1 << fieldIdxInPack);
            }
            writer.EndDocument(docIt->first.first, docIt->first.second);
            expackDocRecord.insert(std::pair<DocKey, fieldmap_t>(docIt->first, fieldMap));
        }
        expackDocRecordVector.push_back(expackDocRecord);
    }
    expackPostingList.first = postList.first;
    expackPostingList.second = expackDocRecordVector;
}

void ExpackPostingMaker::AddToMerge(ExpackPostingList& mergedList, const ExpackPostingList& postList,
                                    bool toAlignBuffer)
{
    if (mergedList.second.empty()) {
        docid_t newBaseId = postList.first;
        mergedList.first = newBaseId;
        ExpackDocRecordVector::const_iterator it = postList.second.begin();
        for (; it != postList.second.end(); ++it) {
            ExpackDocRecord record;
            for (ExpackDocRecord::const_iterator recordIt = it->begin(); recordIt != it->end(); ++recordIt) {
                record.insert(std::make_pair(std::make_pair(recordIt->first.first + newBaseId, recordIt->first.second),
                                             recordIt->second));
            }
            mergedList.second.push_back(record);
        }
    } else {
        ExpackDocRecordVector& leftList = mergedList.second;
        docid_t newBaseId = postList.first;
        const ExpackDocRecordVector& rightList = postList.second;
        ExpackDocRecord& lastRecordOfLeft = leftList.back();
        const ExpackDocRecord& firstRecordOfRight = *(rightList.begin());

        ExpackDocRecordVector::const_iterator it = rightList.begin();
        if (toAlignBuffer && lastRecordOfLeft.size() + firstRecordOfRight.size() <= MAX_DOC_PER_RECORD) {
            for (ExpackDocRecord::const_iterator recordIt = it->begin(); recordIt != it->end(); ++recordIt) {
                lastRecordOfLeft.insert(std::make_pair(
                    std::make_pair(recordIt->first.first + newBaseId, recordIt->first.second), recordIt->second));
            }
            it++;
        }

        for (; it != rightList.end(); ++it) {
            ExpackDocRecord record;
            for (ExpackDocRecord::const_iterator recordIt = it->begin(); recordIt != it->end(); ++recordIt) {
                record.insert(std::make_pair(std::make_pair(recordIt->first.first + newBaseId, recordIt->first.second),
                                             recordIt->second));
            }
            leftList.push_back(record);
        }
    }
}

ExpackPostingMaker::ExpackPostingList
ExpackPostingMaker::SortByWeightMergePostingLists(std::vector<ExpackPostingMaker::ExpackPostingList>& postLists,
                                                  const DocMapper& docMapper)
{
    ExpackPostingList mergedList;
    std::vector<DocInfo> docInfos;
    for (size_t i = 0; i < postLists.size(); i++) {
        docid_t baseDocId = postLists[i].first;
        ExpackDocRecordVector docRecords = postLists[i].second;
        for (size_t j = 0; j < docRecords.size(); j++) {
            ExpackDocMapWithFieldMap docMap = docRecords[j];
            ExpackDocMapWithFieldMap::const_iterator docMapIt = docMap.begin();
            while (docMapIt != docMap.end()) {
                DocKey docKey = docMapIt->first;
                docid_t newDocId = docMapper.GetNewId(baseDocId + docKey.first);
                if (newDocId != INVALID_DOCID) {
                    fieldmap_t fieldMap = docMapIt->second;
                    DocInfo docInfo(newDocId, docKey.second, fieldMap);
                    docInfos.push_back(docInfo);
                }
                docMapIt++;
            }
        }
    }

    sort(docInfos.begin(), docInfos.end(), DocInfoComparator());

    mergedList.first = 0;
    size_t docNum = docInfos.size();
    size_t recordNum = docNum / MAX_DOC_PER_RECORD;

    if (docNum % MAX_DOC_PER_RECORD != 0) {
        recordNum++;
    }
    for (size_t i = 0; i < recordNum; i++) {
        ExpackDocRecord record;
        for (size_t j = 0; j < MAX_DOC_PER_RECORD; j++) {
            size_t index = i * MAX_DOC_PER_RECORD + j;
            if (index >= docNum) {
                mergedList.second.push_back(record);
                return mergedList;
            }

            DocKey docKey = std::make_pair(docInfos[index]._newDocId, docInfos[index]._docPayload);

            record.insert(std::make_pair(docKey, docInfos[index]._fieldMap));
        }
        mergedList.second.push_back(record);
    }
    return mergedList;
}

void ExpackPostingMaker::CreateReclaimedPostingList(ExpackPostingList& destPostingList,
                                                    const ExpackPostingList& srcPostingList, const DocMapper& docMapper)
{
    assert(destPostingList.second.size() == 0);

    docid_t docId = srcPostingList.first;
    do {
        destPostingList.first = docMapper.GetNewId(docId++); // base-docid
    } while (destPostingList.first == INVALID_DOCID);

    const ExpackDocRecordVector& srcDocRecVect = srcPostingList.second;
    ExpackDocRecordPtr newDocRecord(new ExpackDocRecord);

    for (ExpackDocRecordVector::const_iterator it = srcDocRecVect.begin(); it != srcDocRecVect.end(); ++it) {
        const ExpackDocRecord& docRecord = *it;
        for (ExpackDocRecord::const_iterator docRecIter = docRecord.begin(); docRecIter != docRecord.end();
             ++docRecIter) {
            const DocKey& docKey = (*docRecIter).first;
            DocKey newDocKey;
            if (docMapper.GetNewId(docKey.first) != INVALID_DOCID) {
                newDocKey.first = docMapper.GetNewId(docKey.first); // docid
                newDocKey.second = docKey.second;                   // docpayload
                newDocRecord->insert(std::make_pair(newDocKey, (*docRecIter).second));
            }

            if (newDocRecord->size() == MAX_DOC_PER_RECORD) {
                destPostingList.second.push_back(*newDocRecord);
                newDocRecord.reset(new ExpackDocRecord);
            }
        }

        if (newDocRecord->size() > 0) {
            destPostingList.second.push_back(*newDocRecord);
            newDocRecord.reset(new ExpackDocRecord);
        }
    }

    if (newDocRecord->size() > 0) {
        destPostingList.second.push_back(*newDocRecord);
    }
}

} // namespace indexlib::index
