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
#ifndef MULTI_CALL_HEARTBEAT_METAUTIL_H_
#define MULTI_CALL_HEARTBEAT_METAUTIL_H_
#include "aios/network/gig/multi_call/common/HbInfo.h"

namespace multi_call {

/**
 * util class for meta topo access
 **/
class MetaUtil {
public:
    /**
     * add from.extend to to.extend
     * ignore exsited k in to.extend if is_override=false
     **/
    static int addExtendTo(const BizMeta &from, BizMeta *to,
                           bool is_override = false);

    /**
     * add from.biz_meta infos to to.biz_meta
     * do not add if biz_name and version exist in to
     * T = Meta, HeartbeatRequest
     **/
    template <typename T>
    static int addBizMetaTo(const T &from, T *to, bool is_override = false) {
        assert(to != nullptr);
        int new_biz_meta_count = 0;
        for (auto &bizMetaFrom : from.biz_meta()) {
            BizMeta *bizMetaToPtr = nullptr;
            for (auto &bizMetaTo : *(to->mutable_biz_meta())) {
                if (bizMetaFrom.biz_name() == bizMetaTo.biz_name()) {
                    bizMetaToPtr = &bizMetaTo;
                    break;
                }
            }
            if (bizMetaToPtr == nullptr) { // if not exist, create one
                bizMetaToPtr = to->add_biz_meta();
                *bizMetaToPtr = bizMetaFrom;
                ++new_biz_meta_count;
            } else if (is_override) { // replace on same version
                *bizMetaToPtr = bizMetaFrom;
            }
        }
        return new_biz_meta_count;
    }
};

} // namespace multi_call

#endif // MULTI_CALL_HEARTBEAT_METAUTIL_H_
