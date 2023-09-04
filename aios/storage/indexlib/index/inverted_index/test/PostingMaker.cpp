#include "indexlib/index/inverted_index/test/PostingMaker.h"

#include <iostream>
#include <sstream>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/common/numeric_compress/EncoderProvider.h"
#include "indexlib/util/Random.h"

namespace indexlib::index {
namespace {
using indexlibv2::index::DocMapper;
} // namespace
AUTIL_LOG_SETUP(indexlib.index, PostingMaker);

PostingMaker::DocMap PostingMaker::MakeDocMap(const std::string& str)
{
    // format: "docid docPayload, (pos posPayload, pos posPayload, ...); ....

    PostingMaker::DocMap docMap;
    autil::StringTokenizer st(str, ";",
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); ++it) {
        std::string key, value;
        ParseKeyValue(*it, ",", key, value);

        docid_t docId;
        docpayload_t docPayload;
        std::istringstream iss(key);
        iss >> docId;
        iss >> docPayload;

        PosValue posValue;
        if (!value.empty()) {
            value.erase(value.begin());
            value.erase(value.end() - 1);
            autil::StringTokenizer st2(value, ",",
                                       autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);

            for (autil::StringTokenizer::Iterator it2 = st2.begin(); it2 != st2.end(); ++it2) {
                pos_t pos = 0;
                int32_t p = 0;
                pospayload_t posPayload = 0;
                std::istringstream iss(*it2);
                iss >> pos;
                iss >> p;
                posPayload = (pospayload_t)p;
                posValue.push_back(std::make_pair(pos, posPayload));
            }
        }

        docMap[std::make_pair(docId, docPayload)] = posValue;
    }
    return docMap;
}

PostingMaker::PostingList PostingMaker::MakePostingData(uint32_t docCount, docid_t& maxDocId, tf_t& totalTf)
{
    std::vector<uint32_t> numDocsInRecords;
    while (docCount > MAX_DOC_PER_RECORD) {
        numDocsInRecords.push_back(MAX_DOC_PER_RECORD);
        docCount -= MAX_DOC_PER_RECORD;
    }
    if (docCount > 0) {
        numDocsInRecords.push_back(docCount);
    }
    return MakePostingData(numDocsInRecords, maxDocId, totalTf);
}

PostingMaker::PostingList PostingMaker::MakePostingData(const std::vector<uint32_t>& numDocsInRecords,
                                                        docid_t& maxDocId, tf_t& totalTf)
{
    std::vector<std::string> strs;
    docid_t oldDocId = 0;
    totalTf = 0;
    for (size_t i = 0; i < numDocsInRecords.size(); ++i) {
        std::stringstream ss;
        for (uint32_t j = 0; j < numDocsInRecords[i]; ++j) {
            oldDocId += util::dev_urandom() % 19 + 1;
            ss << oldDocId << " ";

            docpayload_t docPayload = util::dev_urandom() % 54321;
            ss << docPayload << ",(";

            int posCount = util::dev_urandom() % 10 + 1;
            if (util::dev_urandom() % 7 == 1) {
                posCount += util::dev_urandom() % 124;
            }
            totalTf += posCount;

            int oldPos = 0;
            for (int k = 0; k < posCount; ++k) {
                ss << (oldPos += util::dev_urandom() % 7 + 1) << " ";
                ss << util::dev_urandom() % 77 << ", ";
            }
            ss << ");";
        }
        strs.push_back(ss.str());
    }
    maxDocId = oldDocId;

    return PostingMaker::MakePostingList(strs);
}

PostingMaker::PostingList PostingMaker::MakePostingList(const std::vector<std::string>& strs)
{
    DocRecordVector docMapRecords;
    for (std::vector<std::string>::const_iterator it = strs.begin(); it != strs.end(); ++it) {
        docMapRecords.push_back(MakeDocMap(*it));
    }
    return std::make_pair(0, docMapRecords);
}

PostingMaker::PostingList PostingMaker::SortByWeightMergePostingLists(std::vector<PostingMaker::PostingList>& postLists,
                                                                      const DocMapper& docMapper)
{
    PostingList mergedList;
    std::vector<DocInfo> docInfos;
    for (size_t i = 0; i < postLists.size(); i++) {
        docid_t baseDocId = postLists[i].first;
        DocRecordVector docRecords = postLists[i].second;
        for (size_t j = 0; j < docRecords.size(); j++) {
            DocMap docMap = docRecords[j];
            DocMap::const_iterator docMapIt = docMap.begin();
            while (docMapIt != docMap.end()) {
                DocKey docKey = docMapIt->first;
                docid_t newDocId = docMapper.GetNewId(baseDocId + docKey.first);
                if (newDocId != INVALID_DOCID) {
                    PosValue posValue = docMapIt->second;
                    DocInfo docInfo(newDocId, docKey.second, posValue);
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
        DocRecord record;
        for (size_t j = 0; j < MAX_DOC_PER_RECORD; j++) {
            size_t index = i * MAX_DOC_PER_RECORD + j;
            if (index >= docNum) {
                mergedList.second.push_back(record);
                return mergedList;
            }

            DocKey docKey = std::make_pair(docInfos[index].mNewDocId, docInfos[index].mDocPayload);

            record.insert(std::make_pair(docKey, docInfos[index].mPosValue));
        }
        mergedList.second.push_back(record);
    }
    return mergedList;
}

PostingMaker::PostingList PostingMaker::MergePostingLists(std::vector<PostingMaker::PostingList>& postLists)
{
    PostingList mergedList;
    for (std::vector<PostingList>::const_iterator it = postLists.begin(); it != postLists.end(); ++it) {
        AddToMerge(mergedList, *it);
    }
    return mergedList;
}

void PostingMaker::AddToMerge(PostingMaker::PostingList& mergedList, const PostingMaker::PostingList& postList,
                              bool toAlignBuffer)
{
    if (mergedList.second.empty()) {
        docid_t newBaseId = postList.first;
        mergedList.first = newBaseId;
        DocRecordVector::const_iterator it = postList.second.begin();
        for (; it != postList.second.end(); ++it) {
            DocRecord record;
            for (DocRecord::const_iterator recordIt = it->begin(); recordIt != it->end(); ++recordIt) {
                record.insert(std::make_pair(std::make_pair(recordIt->first.first + newBaseId, recordIt->first.second),
                                             recordIt->second));
            }
            mergedList.second.push_back(record);
        }
    } else {
        DocRecordVector& leftList = mergedList.second;
        docid_t newBaseId = postList.first;
        const DocRecordVector& rightList = postList.second;
        DocRecord& lastRecordOfLeft = leftList.back();
        const DocRecord& firstRecordOfRight = *(rightList.begin());

        DocRecordVector::const_iterator it = rightList.begin();
        if (toAlignBuffer && lastRecordOfLeft.size() + firstRecordOfRight.size() <= MAX_DOC_PER_RECORD) {
            for (DocRecord::const_iterator recordIt = it->begin(); recordIt != it->end(); ++recordIt) {
                lastRecordOfLeft.insert(std::make_pair(
                    std::make_pair(recordIt->first.first + newBaseId, recordIt->first.second), recordIt->second));
            }
            it++;
        }

        for (; it != rightList.end(); ++it) {
            DocRecord record;
            for (DocRecord::const_iterator recordIt = it->begin(); recordIt != it->end(); ++recordIt) {
                record.insert(std::make_pair(std::make_pair(recordIt->first.first + newBaseId, recordIt->first.second),
                                             recordIt->second));
            }
            leftList.push_back(record);
        }
    }
}

PostingMaker::PostingList PostingMaker::MakePositionData(uint32_t docCount, docid_t& maxDocId, uint32_t& totalTF)
{
    std::vector<uint32_t> numDocsInRecords;
    while (docCount > MAX_DOC_PER_RECORD) {
        numDocsInRecords.push_back(MAX_DOC_PER_RECORD);
        docCount -= MAX_DOC_PER_RECORD;
    }
    if (docCount > 0) {
        numDocsInRecords.push_back(docCount);
    }
    return MakePositionData(numDocsInRecords, maxDocId, totalTF);
}

PostingMaker::PostingList PostingMaker::MakePositionData(const std::vector<uint32_t>& numDocsInRecords,
                                                         docid_t& maxDocId, uint32_t& totalTF)
{
    std::vector<std::string> strs;
    docid_t oldDocId = 0;
    totalTF = 0;
    for (size_t i = 0; i < numDocsInRecords.size(); ++i) {
        std::stringstream ss;
        for (uint32_t j = 0; j < numDocsInRecords[i]; ++j) {
            oldDocId += util::dev_urandom() % 19 + 1;
            ss << oldDocId << " ";
            ss << util::dev_urandom() % 54321 << ", (";

            int posCount = util::dev_urandom() % 10 + 1;
            if (util::dev_urandom() % 7 == 1) {
                posCount += util::dev_urandom() % 124;
            }
            totalTF += posCount;

            int oldPos = 0;
            for (int j = 0; j < posCount; ++j) {
                ss << (oldPos += util::dev_urandom() % 7 + 1) << " ";
                ss << util::dev_urandom() % 77 << ", ";
            }
            ss << ");";
        }
        strs.push_back(ss.str());
    }
    maxDocId = oldDocId;
    return PostingMaker::MakePostingList(strs);
}

void PostingMaker::CreateReclaimedPostingList(PostingList& destPostingList, const PostingList& srcPostingList,
                                              const DocMapper& docMapper)
{
    assert(destPostingList.second.size() == 0);

    docid_t docId = srcPostingList.first;
    do {
        destPostingList.first = docMapper.GetNewId(docId++); // base-docid
    } while (destPostingList.first == INVALID_DOCID);

    const DocRecordVector& srcDocRecVect = srcPostingList.second;
    DocRecordPtr newDocRecord(new DocRecord);

    for (DocRecordVector::const_iterator it = srcDocRecVect.begin(); it != srcDocRecVect.end(); ++it) {
        const DocRecord& docRecord = *it;
        for (DocRecord::const_iterator docRecIter = docRecord.begin(); docRecIter != docRecord.end(); ++docRecIter) {
            const DocKey& docKey = (*docRecIter).first;
            DocKey newDocKey;
            if (docMapper.GetNewId(docKey.first) != INVALID_DOCID) {
                newDocKey.first = docMapper.GetNewId(docKey.first); // docid
                newDocKey.second = docKey.second;                   // docpayload
                newDocRecord->insert(std::make_pair(newDocKey, (*docRecIter).second));
            }

            if (newDocRecord->size() == MAX_DOC_PER_RECORD) {
                destPostingList.second.push_back(*newDocRecord);
                newDocRecord.reset(new DocRecord);
            }
        }

        if (newDocRecord->size() > 0) {
            destPostingList.second.push_back(*newDocRecord);
            newDocRecord.reset(new DocRecord);
        }
    }

    if (newDocRecord->size() > 0) {
        destPostingList.second.push_back(*newDocRecord);
    }
}

/////////////////////////////////////////////////////////////////
// inner functions
void PostingMaker::ParseKeyValue(const std::string& str, const std::string& splite, std::string& key,
                                 std::string& value)
{
    std::string::size_type pos = str.find(splite);
    if (pos != std::string::npos) {
        key = str.substr(0, pos);
        value = str.substr(pos + splite.length());
    } else {
        key = str;
        value = "";
    }
    autil::StringUtil::trim(key);
    autil::StringUtil::trim(value);
}
} // namespace indexlib::index
