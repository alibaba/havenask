#ifndef ISEARCH_FETCHSUMMARYCLAUSE_H
#define ISEARCH_FETCHSUMMARYCLAUSE_H

#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Term.h>
#include <ha3/common/GlobalIdentifier.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class FetchSummaryClause : public ClauseBase {
public:
    FetchSummaryClause();
    ~FetchSummaryClause();
private:
    FetchSummaryClause(const FetchSummaryClause &);
    FetchSummaryClause& operator=(const FetchSummaryClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override {
        return;
    }
    void deserialize(autil::DataBuffer &dataBuffer) override {
        return;
    }
    std::string toString() const override {
        return _originalString;
    }
public:
    void addGid(const std::string &clusterName,
                const GlobalIdentifier &gid)
    {
        _clusterNames.push_back(cluster2ZoneName(clusterName));
        _gids.push_back(gid);
    }
    void addRawPk(const std::string &clusterName,
                  const std::string &rawPk)
    {
        _clusterNames.push_back(cluster2ZoneName(clusterName));
        _rawPks.push_back(rawPk);
    }
    const std::vector<std::string> &getClusterNames() const {
        return _clusterNames;
    }
    const std::vector<GlobalIdentifier> &getGids() const {
        return _gids;
    }
    const std::vector<std::string> &getRawPks() const  {
        return _rawPks;
    }
    size_t getGidCount() const {
        return _gids.size();
    }
    size_t getRawPkCount() const {
        return _rawPks.size();
    }
    size_t getClusterCount() const {
        return _clusterNames.size();
    }
private:
    std::vector<std::string> _clusterNames;
    std::vector<GlobalIdentifier> _gids;
    std::vector<std::string> _rawPks;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FetchSummaryClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_FETCHSUMMARYCLAUSE_H
