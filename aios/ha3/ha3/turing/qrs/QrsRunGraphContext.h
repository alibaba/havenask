#ifndef ISEARCH_TURING_QRSRUNGRAPHCONTEXT_H
#define ISEARCH_TURING_QRSRUNGRAPHCONTEXT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <autil/mem_pool/Pool.h>
#include <suez/turing/common/PoolCache.h>
#include <tensorflow/core/common_runtime/direct_session.h>
#include <suez/turing/common/QueryResource.h>
#include <suez/turing/common/SessionResource.h>

BEGIN_HA3_NAMESPACE(turing);

struct QrsRunGraphContextArgs {
    tensorflow::DirectSession *session = nullptr;
    const std::vector<std::string> *inputs = nullptr;
    tensorflow::SessionResource *sessionResource = nullptr;
    tensorflow::RunOptions runOptions;
    autil::mem_pool::Pool* pool = nullptr;
};

class QrsRunGraphContext
{
public:
    typedef std::function<void(const tensorflow::Status &)> StatusCallback;
public:
    QrsRunGraphContext(const QrsRunGraphContextArgs &args);
    ~QrsRunGraphContext();
private:
    QrsRunGraphContext(const QrsRunGraphContext &);
    QrsRunGraphContext& operator=(const QrsRunGraphContext &);
public:
    void setResults(const common::ResultPtrVector &results) {
        _results = results;
    }
    void setRequest(const common::RequestPtr &request) {
        _request = request;
    }

    void run(std::vector<tensorflow::Tensor> *outputs, tensorflow::RunMetadata *runMetadata,
             StatusCallback done);

    tensorflow::SessionResource *getSessionResource() {
        return _sessionResource;
    }
    void setQueryResource(tensorflow::QueryResource *queryResource) {
        _queryResource = queryResource;
    }
    tensorflow::QueryResource *getQueryResource() {
        return _queryResource;
    }
    autil::mem_pool::Pool *getPool() {
        return _sessionPool;
    }
    void setTracer(common::TracerPtr tracer) {
        if (_queryResource) {
            _queryResource->setTracer(tracer);
        }
    }
private:
    bool getInputs(std::vector<std::pair<std::string, tensorflow::Tensor>> &inputs);
    std::vector<std::string> getTargetNodes() const;
    std::vector<std::string> getOutputNames() const;
    void cleanQueryResource();
private:
    common::RequestPtr _request;
    common::ResultPtrVector _results;
    tensorflow::DirectSession *_session = nullptr;
    const std::vector<std::string> *_inputs = nullptr;
    autil::mem_pool::Pool *_sessionPool = nullptr;
    tensorflow::SessionResource *_sessionResource = nullptr;
    tensorflow::QueryResource *_queryResource = nullptr;
    tensorflow::RunOptions _runOptions;
private:
    static const std::string HA3_REQUEST_TENSOR_NAME;
    static const std::string HA3_RESULTS_TENSOR_NAME;
    static const std::string HA3_RESULT_TENSOR_NAME;

private:
    HA3_LOG_DECLARE();
};

typedef std::shared_ptr<QrsRunGraphContext> QrsRunGraphContextPtr;

END_HA3_NAMESPACE(turing);

#endif
