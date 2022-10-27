#ifndef ISEARCH_SORTDESCRIPTION_H
#define ISEARCH_SORTDESCRIPTION_H

#include <string>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>

BEGIN_HA3_NAMESPACE(common);

class SortDescription
{
public:
    enum RankSortType {
        RS_NORMAL,
        RS_RANK
    };
public:
    SortDescription();
    SortDescription(const std::string &originalString);
    ~SortDescription();
public:
    bool getSortAscendFlag() const;
    void setSortAscendFlag(bool sortAscendFlag);

    const std::string& getOriginalString() const;
    void setOriginalString(const std::string &originalString);

    suez::turing::SyntaxExpr* getRootSyntaxExpr() const;
    void setRootSyntaxExpr(suez::turing::SyntaxExpr* expr);

    bool isRankExpression() const {
        return _type == RS_RANK;
    }

    void setExpressionType(RankSortType type) {
        _type = type;
    }
    RankSortType getExpressionType() const {
        return _type;
    }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    std::string toString() const;
private:
    suez::turing::SyntaxExpr *_rootSyntaxExpr;
    std::string _originalString;
    bool _sortAscendFlag;
    RankSortType _type;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_SORTDESCRIPTION_H
