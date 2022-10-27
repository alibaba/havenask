#ifndef ISEARCH_DOCIDCLAUSE_H
#define ISEARCH_DOCIDCLAUSE_H

#include <map>
#include <set>
#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Term.h>
#include <ha3/common/GlobalIdentifier.h>
#include <ha3/proto/BasicDefs.pb.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

typedef std::vector<GlobalIdentifier> GlobalIdVector;
typedef std::vector<docid_t> DocIdVector;
typedef std::map<versionid_t, GlobalIdVector> GlobalIdVectorMap;
typedef std::vector<Term> TermVector;
typedef std::pair<hashid_t, FullIndexVersion> HashidVersion;
typedef std::set<HashidVersion> HashidVersionSet;

class DocIdClause : public ClauseBase 
{
public:
    DocIdClause()
        : _summaryProfileName(DEFAULT_SUMMARY_PROFILE) 
        , _signature(0)
    {
    }
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override {
        return _originalString;
    }
public:
    void addGlobalId(const GlobalIdentifier &globalId);
    void getGlobalIdVector(const proto::Range &hashIdRange,
                           FullIndexVersion fullVersion,
                           GlobalIdVector &gids);
    void getGlobalIdVector(const proto::Range &hashIdRange,
                           GlobalIdVector &gids);
    void getGlobalIdVectorMap(const proto::Range &hashIdRange,
                              FullIndexVersion fullVersion,
                              GlobalIdVectorMap &globalIdVectorMap);
    void getHashidVersionSet(HashidVersionSet &hashidVersionSet);
    void getHashidVector(std::vector<hashid_t> &hashids);

    void setQueryString(const std::string &queryString) {
        _queryString = queryString;
    }
    const std::string& getQueryString() const {
        return _queryString;
    };
    void setTermVector(const TermVector &termVector) {
        _termVector = termVector;
    }
    const TermVector& getTermVector() const {
        return _termVector;
    }
    std::string getSummaryProfileName() const {
        return _summaryProfileName;
    }
    void setSummaryProfileName(const std::string &summaryProfileName) {
        _summaryProfileName = summaryProfileName;
    }
    const GlobalIdVector& getGlobalIdVector() const {
        return _globalIdVector;
    }
    void setGlobalIdVector(const GlobalIdVector &globalIdVector) {
        _globalIdVector = globalIdVector;
    }

    int64_t getSignature() const { return _signature; }
    void setSignature(int64_t signature) { _signature = signature; }

private:
    GlobalIdVector _globalIdVector;
    TermVector _termVector;
    std::string _queryString;
    std::string _summaryProfileName;
    int64_t _signature;
private:
    HA3_LOG_DECLARE();
};

typedef std::map<std::string, DocIdClause*> DocIdClauseMap;

END_HA3_NAMESPACE(common);

#endif //ISEARCH_DOCIDCLAUSE_H
