#ifndef ISEARCH_OPTIMIZER_H
#define ISEARCH_OPTIMIZER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <ha3/monitor/SessionMetricsCollector.h>

BEGIN_HA3_NAMESPACE(search);

struct OptimizeInitParam {
    const std::string &optimizeOption;
    const common::Request *request;

    OptimizeInitParam(const std::string &option,
                      const common::Request *request)
        : optimizeOption(option)
        , request(request)
    {
    }
};

struct OptimizeParam {
    const common::Request *request;
    IndexPartitionReaderWrapper *readerWrapper;
    monitor::SessionMetricsCollector *sessionMetricsCollector;

    OptimizeParam() {
        request = NULL;
        readerWrapper = NULL;
        sessionMetricsCollector = NULL;
    }
};

class Optimizer;
typedef std::tr1::shared_ptr<Optimizer> OptimizerPtr;

class Optimizer
{
public:
    Optimizer() {}
    virtual ~Optimizer() {}
public:
    virtual bool init(OptimizeInitParam *param) {
        return true;
    }
    virtual OptimizerPtr clone() = 0;
    virtual std::string getName() const = 0;
    virtual bool optimize(OptimizeParam *param) = 0;
    virtual void disableTruncate() = 0;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Optimizer);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_OPTIMIZER_H
