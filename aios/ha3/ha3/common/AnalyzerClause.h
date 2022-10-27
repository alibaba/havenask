#ifndef ISEARCH_ANALYZERCLAUSE_H
#define ISEARCH_ANALYZERCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

// do not need serialize
class AnalyzerClause : public ClauseBase
{
public:
    AnalyzerClause();
    ~AnalyzerClause();
private:
    AnalyzerClause(const AnalyzerClause &);
    AnalyzerClause& operator=(const AnalyzerClause &);
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

private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(AnalyzerClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_ANALYZERCLAUSE_H
