#include "build_service/proto/test/ProtoCreator.h"

#include <cstddef>
#include <memory>

#include "autil/Span.h"
#include "autil/StringTokenizer.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/RangeUtil.h"

using namespace std;
using namespace autil;
using namespace build_service::util;

namespace build_service { namespace proto {

BS_LOG_SETUP(proto, ProtoCreator);

ProtoCreator::ProtoCreator() {}

ProtoCreator::~ProtoCreator() {}

BuildId ProtoCreator::createBuildId(const string& dataTable, generationid_t generationId, const string& appName)
{
    BuildId buildId;
    if (dataTable != "") {
        buildId.set_datatable(dataTable);
    }
    if (generationId != -1) {
        buildId.set_generationid(generationId);
    }
    buildId.set_appname(appName);
    return buildId;
}

PartitionId ProtoCreator::createPartitionId(RoleType role, BuildStep step, uint16_t from, uint16_t to,
                                            const string& dataTable, generationid_t generationId,
                                            const string& clusterNames, const string& mergeConfigName,
                                            const string& taskid, const std::string& appName)
{
    PartitionId partitionId;
    partitionId.set_role(role);
    if (role != ROLE_TASK) {
        partitionId.set_step(step);
    }
    partitionId.mutable_range()->set_from(from);
    partitionId.mutable_range()->set_to(to);
    *partitionId.mutable_buildid() = createBuildId(dataTable, generationId, appName);
    vector<string> clusters = StringTokenizer::tokenize(StringView(clusterNames), ",");
    if (role != ROLE_PROCESSOR) {
        for (size_t i = 0; i < clusters.size(); ++i) {
            *partitionId.add_clusternames() = clusters[i];
        }
    }
    partitionId.set_mergeconfigname(mergeConfigName);
    partitionId.set_taskid(taskid);
    return partitionId;
}

vector<PartitionId> ProtoCreator::createPartitionIds(uint32_t partCount, uint32_t parallelNum, RoleType role,
                                                     BuildStep step, uint16_t from, uint16_t to,
                                                     const string& dataTable, generationid_t gid,
                                                     const string& clusterNames, const string& mergeConfigName)
{
    vector<Range> ranges = RangeUtil::splitRange(from, to, partCount, parallelNum);
    vector<PartitionId> pids;
    pids.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); i++) {
        pids.push_back(createPartitionId(role, step, ranges[i].from(), ranges[i].to(), dataTable, gid, clusterNames,
                                         mergeConfigName));
    }
    return pids;
}

}} // namespace build_service::proto
