#ifndef ISEARCH_SORTCLAUSE_H
#define ISEARCH_SORTCLAUSE_H

#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/SortDescription.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class SortClause : public ClauseBase
{
public:
    SortClause();
    ~SortClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    const std::vector<SortDescription*>& getSortDescriptions() const;
    SortDescription* getSortDescription(uint32_t pos);

    void addSortDescription(SortDescription *sortDescription);
    void addSortDescription(suez::turing::SyntaxExpr* syntaxExpr,
                            bool isRank,
                            bool sortAscendFlag = false);
    void clearSortDescriptions();
private:
    std::vector<SortDescription*> _sortDescriptions;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_SORTCLAUSE_H
 
