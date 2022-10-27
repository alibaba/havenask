#include <suez/turing/common/SessionResource.h>
#include <suez/turing/common/QueryResource.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/searcher/BasicSearcherContext.h>
#include <ha3/common/CommonDef.h>
#include <ha3/util/RangeUtil.h>

using namespace std;
using namespace suez::turing;
using namespace tensorflow;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, BasicSearcherContext);

BasicSearcherContext::BasicSearcherContext(const SearchContextArgs &args,
        const GraphRequest *request,
        GraphResponse *response)
    : GraphSearchContext(args, request, response)
{
}

BasicSearcherContext::~BasicSearcherContext() {
}

void BasicSearcherContext::formatErrorResult() {
    const vector<string> &outputNames = _request->outputNames;
    if (outputNames.size() != 1 || outputNames[0] != HA3_RESULT_TENSOR_NAME) {
        return;
    }
    common::ResultPtr result = common::ResultPtr(new common::Result(new common::MatchDocs()));
    ErrorResult errResult(ERROR_RUN_SEARCHER_GRAPH_FAILED, "run searcher graph failed: " +
                          _errorInfo.errormsg());
    result->addErrorResult(errResult);
    if (_queryResource->getTracerPtr()) {
        result->setTracer(_queryResource->getTracerPtr());
    }

    const suez::ServiceInfo &serviceInfo = _sessionResource->serviceInfo;
    string zoneBizName = serviceInfo.getZoneName()  + "." + _sessionResource->bizName;
    PartitionRange utilRange;
    if (!RangeUtil::getRange(serviceInfo.getPartCount(), serviceInfo.getPartId(), utilRange)) {
        HA3_LOG(ERROR, "get Range failed");
    }
    result->addCoveredRange(zoneBizName, utilRange.first, utilRange.second);

    auto resultTensor = Tensor(DT_VARIANT, TensorShape({}));
    Ha3ResultVariant ha3Result(result, _pool);
    resultTensor.scalar<Variant>()() = ha3Result;
    suez::turing::NamedTensorProto *namedTensor = _graphResponse->add_outputs();
    namedTensor->set_name(HA3_RESULT_TENSOR_NAME);
    resultTensor.AsProtoField(namedTensor->mutable_tensor());
}

END_HA3_NAMESPACE(turing);
