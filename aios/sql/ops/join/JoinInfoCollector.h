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

#include <stdint.h>

#include "sql/proto/SqlSearchInfo.pb.h"

namespace sql {

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

    static void incRightScanTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_rightscantime(joinInfo->rightscantime() + time);
    }

    static void incRightUpdateQueryTime(JoinInfo *joinInfo, uint64_t time) {
        joinInfo->set_rightupdatequerytime(joinInfo->rightupdatequerytime() + time);
    }
};

} // namespace sql
