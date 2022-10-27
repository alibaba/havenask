#ifndef ISEARCH_RANKQUERY_H
#define ISEARCH_RANKQUERY_H

#include <string>
#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/Query.h>

BEGIN_HA3_NAMESPACE(common);

class RankQuery : public Query
{
public:
    RankQuery(const std::string &label);
    RankQuery(const RankQuery &other);
    ~RankQuery();
public:
    void addQuery(QueryPtr queryPtr) override;
    void addQuery(QueryPtr queryPtr, uint32_t rankBoost);

    bool operator == (const Query& query) const override;
    void accept(QueryVisitor *visitor) const override;
    void accept(ModifyQueryVisitor *visitor) override;
    Query *clone() const override;

    std::string getQueryName() const override {
        return "RankQuery";
    }

    void setRankBoost(uint32_t rankBoost, uint32_t pos);
    uint32_t getRankBoost(uint32_t pos) const;

    QueryType getType() const override {
        return RANK_QUERY;
    }
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    void setQueryLabelWithDefaultLevel(const std::string &label) override {
        setQueryLabelBinary(label);
    }
private:
    typedef std::vector<uint32_t> RankBoostVecor;
    RankBoostVecor _rankBoosts;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RANKQUERY_H
