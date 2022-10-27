#ifndef ISEARCH_RANKCLAUSE_H
#define ISEARCH_RANKCLAUSE_H

#include <string>
#include <map>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/RankHint.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

typedef std::map<std::string, int32_t> SingleFieldBoostConfig;
typedef std::map<std::string, SingleFieldBoostConfig> FieldBoostDescription;

class RankClause : public ClauseBase
{
public:
    RankClause();
    ~RankClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setRankProfileName(const std::string &rankProfileName);
    std::string getRankProfileName() const;

    void setFieldBoostDescription(const FieldBoostDescription& des);
    const FieldBoostDescription& getFieldBoostDescription() const;

    void setRankHint(RankHint* rankHint);
    const RankHint* getRankHint() const;
private:
    std::string _rankProfileName;
    FieldBoostDescription _fieldBoostDescription;
    RankHint *_rankHint;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RANKCLAUSE_H
