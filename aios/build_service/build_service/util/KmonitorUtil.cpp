#include "build_service/util/KmonitorUtil.h"
#include "build_service/util/EnvUtil.h"
#include "build_service/proto/ProtoUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace build_service::proto;

namespace build_service {
namespace util {

void KmonitorUtil::getTags(const proto::PartitionId &pid, kmonitor::MetricsTags &tags)
{ 
    string generationId = StringUtil::toString(pid.buildid().generationid());
    tags.AddTag("generation", generationId);
    string roleName = ProtoUtil::toRoleString(pid.role());
    tags.AddTag("role", roleName);
    string partition = StringUtil::toString(pid.range().from());
    partition += "_" + StringUtil::toString(pid.range().to());
    tags.AddTag("partition", partition);

    if (pid.role() == RoleType::ROLE_PROCESSOR)
    {
        string step = ProtoUtil::toStepString(pid);
        tags.AddTag("step", step);
    }
    if (pid.role() == RoleType::ROLE_BUILDER)
    {
        string clusterName = pid.clusternames(0);
        tags.AddTag("cluster", clusterName);
        string step = ProtoUtil::toStepString(pid);
        tags.AddTag("step", step);
    }
    if (pid.role() == RoleType::ROLE_MERGER)
    {
        string clusterName = pid.clusternames(0);
        tags.AddTag("cluster", clusterName);
        tags.AddTag("mergeConfig", pid.mergeconfigname());
    }
    string tableName = StringUtil::toString(pid.buildid().datatable());
    tags.AddTag("table_name", tableName);
    tags.AddTag("app", getApp());
}

void KmonitorUtil::getTags(const Range &range, const string& clusterName,
                           const string& dataTable,
                           generationid_t generationId,
                           kmonitor::MetricsTags& tags) {
    tags.AddTag("generation", StringUtil::toString(generationId));
    string partition = StringUtil::toString(range.from());
    partition += "_" + StringUtil::toString(range.to());
    tags.AddTag("partition", partition);
    tags.AddTag("cluster", clusterName);
    tags.AddTag("table_name", dataTable);
    tags.AddTag("app", getApp());
}

string KmonitorUtil::getApp()
{
    string app = "build_service";
    // for compatity
    EnvUtil::getValueFromEnv("KMONITOR_ADAPTER_APP", app);
    // new env will override old env
    EnvUtil::getValueFromEnv("build_service_app", app);
    return app;
}

}
}
