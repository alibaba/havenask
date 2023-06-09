/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stddef.h>
#include <map>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/turing/common/Ha3BizMeta.h"

namespace isearch {
namespace turing {
class Ha3BizMeta;
}  // namespace turing
}  // namespace isearch

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor


namespace suez {
namespace turing {
class SorterManager;

typedef std::shared_ptr<SorterManager> SorterManagerPtr;
typedef std::map<std::string, SorterManagerPtr> ClusterSorterManagers;
typedef std::shared_ptr<ClusterSorterManagers> ClusterSorterManagersPtr;
}
}

namespace isearch {
namespace turing {
class QrsRunGraphContext;


typedef std::shared_ptr<QrsRunGraphContext> QrsRunGraphContextPtr;
} // namespace turing
} // namespace isearch

namespace isearch {
namespace qrs {

class QrsProcessor;

typedef std::shared_ptr<QrsProcessor> QrsProcessorPtr;

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

    void setHa3BizMeta(turing::Ha3BizMeta *ha3BizMeta) {
        _ha3BizMeta = ha3BizMeta;
    }
    turing::Ha3BizMeta *getHa3BizMeta() {
        return _ha3BizMeta;
    }

public:
    virtual void fillSummary(const common::RequestPtr &requestPtr,
                             const common::ResultPtr &resultPtr);
public:

    virtual std::string getName() const;
    const KeyValueMap &getParams() const;
    std::string getParam(const std::string &key) const;

public:
    static std::unique_ptr<indexlibv2::config::ITabletSchema>
    loadSchema(const std::string &schemaFilePath, config::ResourceReader *resourceReader);

protected:
    common::Tracer *_tracer;
    monitor::SessionMetricsCollectorPtr _metricsCollectorPtr;
    common::TimeoutTerminator *_timeoutTerminator;
    turing::QrsRunGraphContextPtr _runGraphContext;
    turing::Ha3BizMeta *_ha3BizMeta;
private:
    KeyValueMap _keyValues;
    QrsProcessorPtr _nextProcessor;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace qrs
} // namespace isearch
