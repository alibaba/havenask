#ifndef ISEARCH_FAKEINDEXPARAM_H
#define ISEARCH_FAKEINDEXPARAM_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(common);

struct FakeIndex {
    FakeIndex() {
        versionId = 0;
    }
    ~FakeIndex() {
    }
    std::map<std::string, std::string> indexes;

    // summary format:
    //    field1,field2,field3,...,fieldN;   ---> doc1
    //    field1,field2,field3,...,fieldN;   ---> doc2
    std::string summarys;
    // attribute format:
    //    attrName:attrType:doc1,doc2,...,docN;
    //    attrName:attrType:doc1,doc2,...,docN;
    // !!! pay attention !!!
    // single attribute, doc sep is ","
    // multi attribute, doc sep is "#", multi value sep is ","
    std::string attributes;
    // deletion map format:
    //    doc1,doc2,...,docN
    std::string deletedDocIds;
    std::string partitionMeta;
    // segmentDocCount1,segmentDocCount2|rtSegmentDocCount
    std::string segmentDocCounts;
    int32_t versionId;
    uint32_t totalDocCount;
    std::vector<std::string> tablePrefix;

    void clear() {
        indexes.clear();
        summarys.clear();
        attributes.clear();
        deletedDocIds.clear();
        partitionMeta.clear();
        versionId = 0;
        tablePrefix.clear();
    }
};

struct TestParamBase {
    FakeIndex fakeIndex;
    uint32_t numTerms;

    // match data string format:
    //   firstOcc:docPayload:fieldMap:isMatched,firstOcc:docPayload:fieldMap:isMatched;
    //   firstOcc:docPayload:fieldMap:isMatched,firstOcc:docPayload:fieldMap:isMatched;
    std::string matchDataStr;

    // simple match data string format:
    //   isMatched,isMatched,...,isMatched;  <-- doc0
    //   isMatched,isMatched,...,isMatched;  <-- doc1
    // for example:
    //     0,1,1,0;
    //     1,1,1,0;
    std::string simpleMatchDataStr;

    // format: docid,docid,docid...
    std::string docIdStr;

    // request
    std::string requestStr;

    TestParamBase() 
        : numTerms(0)
        , matchDataStr("")
        , simpleMatchDataStr("")
        , docIdStr("")
        , requestStr("")
    {}
};

struct ScorerTestParam : public TestParamBase {
    // format: score1,score2,score3...
    std::string expectedScoreStr;

    ScorerTestParam()
        : TestParamBase()
        , expectedScoreStr("")
    {}
};

struct SorterTestParam : public TestParamBase {
    // score1,score2,score3,...
    std::string scoreStr;
    uint32_t requiredTopK;
    uint32_t resultSourceNum;
    // doc1,doc2,doc3...
    std::string expectedDocIdStr;
    uint32_t totalMatchCount;
    uint32_t actualMatchCount;

    std::string location;

    SorterTestParam()
        : scoreStr("")
        , requiredTopK(0)
        , resultSourceNum(1)
        , expectedDocIdStr("")
        , totalMatchCount(0)
        , actualMatchCount(0)
        , location("searcher")
    {}
};


END_HA3_NAMESPACE(common);

#endif //ISEARCH_FAKEINDEXPARAM_H
