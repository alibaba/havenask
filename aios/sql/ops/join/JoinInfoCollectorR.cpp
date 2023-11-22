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
#include "sql/ops/join/JoinInfoCollectorR.h"

#include "sql/proto/SqlSearchInfo.pb.h"

namespace sql {

const std::string JoinInfoCollectorR::RESOURCE_ID = "join_info_collector_r";

JoinInfoCollectorR::JoinInfoCollectorR() {}

JoinInfoCollectorR::~JoinInfoCollectorR() {}

void JoinInfoCollectorR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

navi::ErrorCode JoinInfoCollectorR::init(navi::ResourceInitContext &ctx) {
    _joinInfo = std::make_unique<JoinInfo>();
    return navi::EC_NONE;
}

void JoinInfoCollectorR::incHashTime(uint64_t time) {
    _joinInfo->set_totalhashtime(_joinInfo->totalhashtime() + time);
}

void JoinInfoCollectorR::incCreateTime(uint64_t time) {
    _joinInfo->set_totalcreatetime(_joinInfo->totalcreatetime() + time);
}

void JoinInfoCollectorR::incLeftHashCount(uint64_t count) {
    _joinInfo->set_totallefthashcount(_joinInfo->totallefthashcount() + count);
}

void JoinInfoCollectorR::incRightHashCount(uint64_t count) {
    _joinInfo->set_totalrighthashcount(_joinInfo->totalrighthashcount() + count);
}

void JoinInfoCollectorR::incHashMapSize(uint64_t size) {
    _joinInfo->set_hashmapsize(_joinInfo->hashmapsize() + size);
}

void JoinInfoCollectorR::incComputeTimes() {
    _joinInfo->set_totalcomputetimes(_joinInfo->totalcomputetimes() + 1);
}

void JoinInfoCollectorR::incJoinCount(uint64_t count) {
    _joinInfo->set_totaljoincount(_joinInfo->totaljoincount() + count);
}

void JoinInfoCollectorR::incTotalTime(uint64_t time) {
    _joinInfo->set_totalusetime(_joinInfo->totalusetime() + time);
}

void JoinInfoCollectorR::incJoinTime(uint64_t time) {
    _joinInfo->set_totaljointime(_joinInfo->totaljointime() + time);
}

void JoinInfoCollectorR::incInitTableTime(uint64_t time) {
    _joinInfo->set_totalinittabletime(_joinInfo->totalinittabletime() + time);
}

void JoinInfoCollectorR::incEvaluateTime(uint64_t time) {
    _joinInfo->set_totalevaluatetime(_joinInfo->totalevaluatetime() + time);
}

void JoinInfoCollectorR::incRightScanTime(uint64_t time) {
    _joinInfo->set_rightscantime(_joinInfo->rightscantime() + time);
}

void JoinInfoCollectorR::incRightUpdateQueryTime(uint64_t time) {
    _joinInfo->set_rightupdatequerytime(_joinInfo->rightupdatequerytime() + time);
}

void JoinInfoCollectorR::incTotalLeftInputTable(size_t count) {
    _joinInfo->set_totalleftinputcount(_joinInfo->totalleftinputcount() + count);
}

void JoinInfoCollectorR::incTotalRightInputTable(size_t count) {
    _joinInfo->set_totalrightinputcount(_joinInfo->totalrightinputcount() + count);
}

REGISTER_RESOURCE(JoinInfoCollectorR);

} // namespace sql
