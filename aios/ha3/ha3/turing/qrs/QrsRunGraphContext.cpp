#include <ha3/turing/qrs/QrsRunGraphContext.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>

using namespace std;
using namespace suez;
using namespace suez::turing;
using namespace tensorflow;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, QrsRunGraphContext);
const std::string QrsRunGraphContext::HA3_REQUEST_TENSOR_NAME = "ha3_request";
const std::string QrsRunGraphContext::HA3_RESULTS_TENSOR_NAME = "ha3_results";
const std::string QrsRunGraphContext::HA3_RESULT_TENSOR_NAME = "ha3_result";

QrsRunGraphContext::QrsRunGraphContext(const QrsRunGraphContextArgs &args)
    : _session(args.session)
    , _inputs(args.inputs)
    , _sessionPool(args.pool)
    , _sessionResource(args.sessionResource)
    , _runOptions(args.runOptions)
{
}

QrsRunGraphContext::~QrsRunGraphContext() {
    cleanQueryResource();
}

void QrsRunGraphContext::run(vector<Tensor> *outputs, RunMetadata *runMetadata, StatusCallback done) {
    vector<pair<string, Tensor>> inputs;
    if (!getInputs(inputs)) {
        done(errors::Internal("prepare inputs failed"));
        return;
    }
    auto targets = getTargetNodes();
    auto outputNames = getOutputNames();
    auto s = _session->Run(_runOptions, inputs, outputNames, targets, outputs, runMetadata);
    done(s);
}

bool QrsRunGraphContext::getInputs(vector<pair<string, Tensor>> &inputs) {
    auto requestTensor = Tensor(DT_VARIANT, TensorShape({}));
    Ha3RequestVariant requestVariant(_request, _sessionPool);
    requestTensor.scalar<Variant>()() = requestVariant;
    inputs.emplace_back(make_pair(HA3_REQUEST_TENSOR_NAME, requestTensor));
    auto resultsTensor = Tensor(DT_VARIANT, TensorShape({(long long int)_results.size()}));
    auto flat = resultsTensor.flat<Variant>();
    for (size_t i = 0; i < _results.size(); i++) {
        Ha3ResultVariant resultVariant(_results[i], _sessionPool);
        flat(i) = resultVariant;
    }
    inputs.emplace_back(make_pair(HA3_RESULTS_TENSOR_NAME, resultsTensor));
    return true;
}

vector<string> QrsRunGraphContext::getTargetNodes() const {
    vector<string> targets;
    targets.push_back(HA3_RESULT_TENSOR_NAME);
    return targets;
}

std::vector<std::string> QrsRunGraphContext::getOutputNames() const {
    vector<string> output;
    output.push_back(HA3_RESULT_TENSOR_NAME);
    return output;
}


void QrsRunGraphContext::cleanQueryResource() {
    int64_t runId = _runOptions.run_id().value();
    if (_sessionResource) {
        _sessionResource->removeQueryResource(runId);
    }
}

END_HA3_NAMESPACE(turing);
