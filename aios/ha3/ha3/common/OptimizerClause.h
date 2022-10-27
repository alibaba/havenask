#ifndef ISEARCH_OPTIMIZERCLAUSE_H
#define ISEARCH_OPTIMIZERCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class OptimizerClause : public ClauseBase
{
public:
    OptimizerClause();
    ~OptimizerClause();
private:
    OptimizerClause(const OptimizerClause &);
    OptimizerClause& operator=(const OptimizerClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    uint32_t getOptimizeCount() const {
        return _optimizeNames.size();
    }
    const std::string &getOptimizeName(uint32_t pos) const {
        assert(pos < _optimizeNames.size());
        return _optimizeNames[pos];
    }
    void addOptimizerName(const std::string &name) {
        _optimizeNames.push_back(name);
    }

    const std::string &getOptimizeOption(uint32_t pos) const {
        assert(pos < _optimizeOptions.size());
        return _optimizeOptions[pos];
    }
    void addOptimizerOption(const std::string &option) {
        _optimizeOptions.push_back(option);
    }
private:
    std::vector<std::string> _optimizeNames;
    std::vector<std::string> _optimizeOptions;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(OptimizerClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_OPTIMIZERCLAUSE_H
