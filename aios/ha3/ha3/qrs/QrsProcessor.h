#ifndef ISEARCH_QRSPROCESSOR_H
#define ISEARCH_QRSPROCESSOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <kmonitor/client/MetricsReporter.h>

BEGIN_HA3_NAMESPACE(turing);
class QrsRunGraphContext;
typedef std::shared_ptr<QrsRunGraphContext> QrsRunGraphContextPtr;
END_HA3_NAMESPACE(turing);

BEGIN_HA3_NAMESPACE(qrs);

class QrsProcessor;
typedef std::tr1::shared_ptr<QrsProcessor> QrsProcessorPtr;

struct QrsProcessorInitParam {
    QrsProcessorInitParam()
        : keyValues(NULL)
        , resourceReader(NULL)
        , metricsReporter(NULL)
    {}

    const KeyValueMap *keyValues;
    config::ResourceReader *resourceReader;
    kmonitor::MetricsReporter *metricsReporter;
    suez::turing::ClusterSorterManagersPtr clusterSorterManagersPtr;
};


class QrsProcessor
{
public:
    QrsProcessor();
    QrsProcessor(const QrsProcessor &processor);
    virtual ~QrsProcessor();
public:
    virtual void process(common::RequestPtr &requestPtr,
                         common::ResultPtr &resultPtr);
    virtual QrsProcessorPtr clone() = 0;

    virtual bool init(const QrsProcessorInitParam &param);

    virtual bool init(const KeyValueMap &keyValues,
                      config::ResourceReader *resourceReader);

    void setNextProcessor(QrsProcessorPtr processor);
    QrsProcessorPtr getNextProcessor();
    void setTracer(common::Tracer *tracer) {
        _tracer = tracer;
    }

    void setRunGraphContext(const turing::QrsRunGraphContextPtr &context) {
        _runGraphContext = context;
    }

    void setSessionMetricsCollector(
            monitor::SessionMetricsCollectorPtr &collectorPtr)
    {
        _metricsCollectorPtr = collectorPtr;
    }

    monitor::SessionMetricsCollectorPtr getSessionMetricsCollector() const {
        return _metricsCollectorPtr;
    }

    common::Tracer* getTracer() const {
        return _tracer;
    }

    void setTimeoutTerminator(common::TimeoutTerminator *timeoutTerminator);


public:
    virtual void fillSummary(const common::RequestPtr &requestPtr,
                             const common::ResultPtr &resultPtr);
public:

    virtual std::string getName() const;
    const KeyValueMap &getParams() const;
    std::string getParam(const std::string &key) const;

protected:
    common::Tracer *_tracer;
    monitor::SessionMetricsCollectorPtr _metricsCollectorPtr;
    common::TimeoutTerminator *_timeoutTerminator;
    turing::QrsRunGraphContextPtr _runGraphContext;
private:
    KeyValueMap _keyValues;
    QrsProcessorPtr _nextProcessor;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(qrs);

#endif //ISEARCH_QRSPROCESSOR_H
