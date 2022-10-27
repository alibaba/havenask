#include <ha3/turing/ops/Ha3CavaScorerOp.h>
#include <limits>
#include <vector>
#include <autil/Log.h>
#include <autil/TimeUtility.h>
#include <matchdoc/MatchDocAllocator.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/ops/SearcherQueryResource.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(turing);

class Ha3ScoringProviderBuilder : public suez::turing::ScoringProviderBuilder {
public:
    FunctionProvider *getFunctionProvider() override {
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(_queryResource);
        if (!searcherQueryResource) {
            HA3_LOG(ERROR, "ha3 searcher query resource unavailable");
            return nullptr;
        }
        return &(searcherQueryResource->partitionResource->functionProvider);
    }
private:
    HA3_LOG_DECLARE();
};

ScoringProviderBuilderPtr Ha3CavaScorerOp::createScoringProviderBuilder() {
    return ScoringProviderBuilderPtr(new Ha3ScoringProviderBuilder());
}

HA3_LOG_SETUP(turing, Ha3CavaScorerOp);
HA3_LOG_SETUP(turing, Ha3ScoringProviderBuilder);

REGISTER_KERNEL_BUILDER(Name("Ha3CavaScorerOp").Device(DEVICE_CPU), Ha3CavaScorerOp);

END_HA3_NAMESPACE(turing);
