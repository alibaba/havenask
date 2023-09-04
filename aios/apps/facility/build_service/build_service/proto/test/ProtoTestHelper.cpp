#include "build_service/proto/test/ProtoTestHelper.h"

using namespace std;
namespace build_service { namespace proto {
BS_LOG_SETUP(proto, ProtoTestHelper);

BuilderTarget ProtoTestHelper::createBuilderTarget(const string& configPath, int64_t startFromCheckpoint,
                                                   int64_t stopToCheckpoint)

{
    BuilderTarget target;
    target.set_configpath(configPath);
    target.set_starttimestamp(startFromCheckpoint);
    if (stopToCheckpoint > 0) {
        target.set_stoptimestamp(stopToCheckpoint);
    }
    return target;
}

BuilderCurrent ProtoTestHelper::createBuilderCurrent(const BuilderTarget& target, const string& address)
{
    BuilderCurrent current;
    *current.mutable_targetstatus() = target;
    return current;
}

}} // namespace build_service::proto
