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
/**
 * =====================================================================================
 *
 *       Filename:  cm_central_server.h
 *
 *    Description:
 *
 *        Version:  0.1
 *        Created:  2012-08-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __CM_CENTRAL_SERVER_H_
#define __CM_CENTRAL_SERVER_H_

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_hb.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_sub.h"

namespace cm_basic {

class CMCentralServer : public cm_basic::CMCentralHb, public cm_basic::CMCentralSub
{
public:
    CMCentralServer() {}
    virtual ~CMCentralServer() {}
    bool isEmpty()
    {
        autil::ScopedReadLock lock(_rwlock);
        return _cluster_map.empty();
    }
};

typedef std::shared_ptr<CMCentralServer> CMCentralServerPtr;

} // namespace cm_basic

#endif
