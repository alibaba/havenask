#ifndef ISEARCH_FINALSORTCLAUSE_H
#define ISEARCH_FINALSORTCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class FinalSortClause : public ClauseBase
{
public:
    static const std::string DEFAULT_SORTER;
public:
    FinalSortClause();
    ~FinalSortClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
private:
    FinalSortClause(const FinalSortClause &);
    FinalSortClause& operator=(const FinalSortClause &);
public:
    const std::string& getSortName() const {
        return _sortName;
    }
    void setSortName(const std::string& sortName) {
        _sortName = sortName;
    }
    bool useDefaultSort() const {
        return _sortName == DEFAULT_SORTER;
    }
    void addParam(const std::string& key, const std::string& value) {
        _params[key] = value;
    }
    const KeyValueMap& getParams() const {
        return _params;
    }
private:
    std::string _sortName;
    KeyValueMap _params;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(FinalSortClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_FINALSORTCLAUSE_H
