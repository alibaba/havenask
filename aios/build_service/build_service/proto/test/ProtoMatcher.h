#ifndef ISEARCH_BS_PROTOMATCHER_H
#define ISEARCH_BS_PROTOMATCHER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/test/unittest.h"
#include <cstdlib>
namespace build_service {
namespace proto {

MATCHER_P2(RangeEq, from, to, "") {
    *result_listener << arg.ShortDebugString();
    return arg.from() == (uint16_t)from && arg.to() == (uint16_t)to;
}

MATCHER_P2(MergeTaskEq, timestamp, strategyName, "") {
    *result_listener << arg.ShortDebugString();
    return abs(timestamp - arg.timestamp()) < 2 && strategyName == arg.mergeconfigname();

}

MATCHER_P(RawBuildIdEq, buildId, "") {
    *result_listener << arg.ShortDebugString();
    return buildId == arg;
}

MATCHER_P2(BuildIdEq, dataTableName, generationId, "") {
    *result_listener << arg.ShortDebugString();
    return dataTableName == arg.datatablename() &&
        generationId == arg.generationid();
}

MATCHER_P8(PartitionIdEq, role, step, from, to, generationId, dataTable, clusterName, mergeConfigName, "") {
    *result_listener << arg.ShortDebugString();
    if (arg.role() == ROLE_BUILDER || arg.role() == ROLE_MERGER) {
        if (arg.clusternames_size() != 1 || arg.clusternames(0) != clusterName) {
            return false;
        }
    }
    return role == arg.role()
        && step == arg.step()
        && (uint32_t)from == arg.range().from()
        && (uint32_t)to == arg.range().to()
        && (uint32_t)generationId == arg.buildid().generationid()
        && dataTable == arg.buildid().datatable()
        && mergeConfigName == arg.mergeconfigname();

}

}
}

#endif //ISEARCH_BS_PROTOMATCHER_H
