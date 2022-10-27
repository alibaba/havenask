#ifndef ISEARCH_RANKSORTCLAUSE_H
#define ISEARCH_RANKSORTCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/RankSortDescription.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class RankSortClause : public ClauseBase
{
public:
    RankSortClause();
    ~RankSortClause();
private:
    RankSortClause(const RankSortClause &);
    RankSortClause& operator=(const RankSortClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void addRankSortDesc(RankSortDescription* desc) {
        _descs.push_back(desc);
    }
    size_t getRankSortDescCount() const {
        return _descs.size();
    }
    const RankSortDescription* getRankSortDesc(size_t idx) const {
        assert (idx < _descs.size());
        return _descs[idx];
    }
private:
    std::vector<RankSortDescription*> _descs;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(RankSortClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_RANKSORTCLAUSE_H
