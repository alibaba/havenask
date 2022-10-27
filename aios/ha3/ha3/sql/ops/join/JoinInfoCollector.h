#pragma once

#include <ha3/sql/common/common.h>
#include <ha3/sql/proto/SqlSearchInfoCollector.h>

BEGIN_HA3_NAMESPACE(sql);

class JoinInfoCollector {
public:
    static void incHashTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_totalhashtime(joinInfo->totalhashtime() + time);
    }

    static void incCreateTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_totalcreatetime(joinInfo->totalcreatetime() + time);
    }

    static void incLeftHashCount(JoinInfo *joinInfo, uint64_t count) {
        joinInfo->set_totallefthashcount(joinInfo->totallefthashcount() + count);
    }

    static void incRightHashCount(JoinInfo *joinInfo, uint64_t count) {
        joinInfo->set_totalrighthashcount(joinInfo->totalrighthashcount() + count);
    }

    static void incHashMapSize(JoinInfo *joinInfo, uint64_t size) {
        joinInfo->set_hashmapsize(joinInfo->hashmapsize() + size);
    }

    static void incComputeTimes(JoinInfo *joinInfo) {
        joinInfo->set_totalcomputetimes(joinInfo->totalcomputetimes() + 1);
    }

    static void incJoinCount(JoinInfo *joinInfo, uint64_t count) {
        joinInfo->set_totaljoincount(joinInfo->totaljoincount() + count);
    }

    static void incTotalTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_totalusetime(joinInfo->totalusetime() + time);
    }

    static void incJoinTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_totaljointime(joinInfo->totaljointime() + time);
    }

    static void incInitTableTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_totalinittabletime(joinInfo->totalinittabletime() + time);
    }

    static void incEvaluateTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_totalevaluatetime(joinInfo->totalevaluatetime() + time);
    }
};

END_HA3_NAMESPACE(sql);
