#include "indexlib/index/normal/attribute/test/attribute_merger_test_util.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::merger;

namespace indexlib { namespace index {

MockSegmentDirectory::MockSegmentDirectory(const DirectoryPtr& rootDir, const Version& version)
    : merger::SegmentDirectory(rootDir, version)
{
}

void MockReclaimMap::Init(const vector<uint32_t>& docCounts, const vector<set<docid_t>>& delDocIdSets)
{
    uint32_t docCount = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        docCount += docCounts[i];
    }
    mDocIdArray.resize(docCount);

    docid_t id = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        for (size_t j = 0; j < docCounts[i]; ++j) {
            set<docid_t>::const_iterator it = delDocIdSets[i].find(j);
            if (it == delDocIdSets[i].end()) {
                mDocIdArray[id] = mNewDocId++;
            } else {
                mDocIdArray[id] = INVALID_DOCID;
                mDeletedDocCount++;
            }
            id++;
        }
    }
}

MockBufferedFileWriter::MockBufferedFileWriter(vector<string>& dataVec) : mDataVec(dataVec) {}

FSResult<size_t> MockBufferedFileWriter::Write(const void* src, size_t lenToWrite) noexcept
{
    mDataVec.push_back(string((char*)src, lenToWrite));
    return {FSEC_OK, lenToWrite};
}

AttributeConfigPtr AttributeMergerTestUtil::CreateAttributeConfig(FieldType type, bool isMultiValue)
{
    FieldConfigPtr fieldConfig(new FieldConfig("mock_name", type, isMultiValue));
    AttributeConfigPtr attrConfig(new AttributeConfig());
    attrConfig->Init(fieldConfig);
    attrConfig->SetUpdatableMultiValue(true);
    return attrConfig;
}

//////////////////////////////////////////////////////////////////////
void AttributeMergerTestUtil::CreateDocCountsAndDelDocIdSets(const string& segMergeInfosString,
                                                             vector<uint32_t>& docCounts,
                                                             vector<set<docid_t>>& delDocIdSets)
{
    StringTokenizer stSegs;
    stSegs.tokenize(segMergeInfosString, ";", StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < stSegs.getNumTokens(); ++i) {
        StringTokenizer stInOneSeg;
        stInOneSeg.tokenize(stSegs[i], "(");

        uint32_t docCount = StringUtil::fromString<uint32_t>(stInOneSeg[0]);
        docCounts.push_back(docCount);

        StringTokenizer stDelDocIds;
        string delDocsString = stInOneSeg[1].substr(0, stInOneSeg[1].size() - 1);
        stDelDocIds.tokenize(delDocsString, ",", StringTokenizer::TOKEN_IGNORE_EMPTY);

        set<docid_t> delDocIdSet;
        for (size_t j = 0; j < stDelDocIds.getNumTokens(); ++j) {
            delDocIdSet.insert(StringUtil::fromString<docid_t>(stDelDocIds[j]));
        }
        delDocIdSets.push_back(delDocIdSet);
    }
}

ReclaimMapPtr AttributeMergerTestUtil::CreateReclaimMap(const string& segMergeInfosString)
{
    vector<uint32_t> docCounts;
    vector<set<docid_t>> delDocIdSets;
    CreateDocCountsAndDelDocIdSets(segMergeInfosString, docCounts, delDocIdSets);
    MockReclaimMap* mockReclaimMap = new MockReclaimMap();
    mockReclaimMap->Init(docCounts, delDocIdSets);
    ReclaimMapPtr reclaimMap(mockReclaimMap);

    return reclaimMap;
}

SegmentMergeInfos AttributeMergerTestUtil::CreateSegmentMergeInfos(const string& segMergeInfosString)
{
    vector<uint32_t> docCounts;
    vector<set<docid_t>> delDocIdSets;
    CreateDocCountsAndDelDocIdSets(segMergeInfosString, docCounts, delDocIdSets);

    SegmentMergeInfos segMergeInfos;
    docid_t baseDocId = 0;
    for (size_t i = 0; i < docCounts.size(); ++i) {
        SegmentMergeInfo mergeInfo;
        mergeInfo.segmentId = i;
        mergeInfo.segmentInfo.docCount = docCounts[i];
        mergeInfo.deletedDocCount = delDocIdSets[i].size();
        mergeInfo.baseDocId = baseDocId;
        segMergeInfos.push_back(mergeInfo);

        baseDocId += docCounts[i];
    }
    return segMergeInfos;
}
}} // namespace indexlib::index
