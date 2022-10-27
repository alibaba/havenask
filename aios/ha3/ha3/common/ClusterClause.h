#ifndef ISEARCH_CLUSTERCLAUSE_H
#define ISEARCH_CLUSTERCLAUSE_H

#include <string>
#include <map>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ClauseBase.h>
#include <ha3/util/RangeUtil.h>

BEGIN_HA3_NAMESPACE(common);

typedef std::map<std::string, util::RangeVec> ClusterPartIdPairsMap;
typedef std::map<std::string, std::vector<hashid_t> > ClusterPartIdsMap;

class ClusterClause : public ClauseBase
{
public:
    ClusterClause();
    ~ClusterClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override {
        return _originalString;
    }
public:
    const ClusterPartIdsMap& getClusterPartIdsMap() const;
    const ClusterPartIdPairsMap& getClusterPartIdPairsMap() const;
    bool emptyPartIds() const;

    /**deprecated**/
    void addClusterPartIds(const std::string &clusterName,
                           const std::vector<hashid_t> &partids);

    void addClusterPartIds(const std::string &clusterName,
                           const std::vector<std::pair<hashid_t, hashid_t> > &partids);

    bool getClusterPartIds(const std::string &clusterName,
                           std::vector<std::pair<hashid_t, hashid_t> > &partids) const;

    void clearClusterName();
    size_t getClusterNameCount() const;
    void addClusterName(const std::string &clusterName);
    std::string getClusterName(size_t index = 0) const;
    const std::vector<std::string> &getClusterNameVector() const;
    void setTotalSearchPartCnt(uint32_t partCnt);
    uint32_t getToalSearchPartCnt() const;
private:
    void serializeExtFields(autil::DataBuffer &dataBuffer) const;
    void deserializeExtFields(autil::DataBuffer &dataBuffer);
private:
    ClusterPartIdsMap _clusterPartIdsMap;
    ClusterPartIdPairsMap _clusterPartIdPairsMap;
    std::vector<std::string> _clusterNameVec;
    uint32_t _totalSearchPartCnt;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ClusterClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_CLUSTERCLAUSE_H
