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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/Thread.h"
#include "indexlib/framework/TabletPortalBase.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/httplib.h"

namespace indexlibv2::framework {

class HttpTabletPortal : public TabletPortalBase
{
public:
    HttpTabletPortal(int32_t threadNum = 4);
    ~HttpTabletPortal();

public:
    bool IsRunning() const override;

    /* -1 means use random port */
    bool Start(const indexlib::util::KeyValueMap& param);
    void Stop();
    bool HasError() const { return _hasError; }

    using TabletPortalBase::SetAllowBuildDoc;
    using TabletPortalBase::SetAllowDuplicatedTabletName;

private:
    void RegisterMethod_PrintTabletNames();
    void RegisterMethod_QueryTabletSchema();
    void RegisterMethod_QueryTabletInfoSummmary();
    void RegisterMethod_QueryTabletInfoDetail();
    void RegisterMethod_Stop();
    void RegisterMethod_SearchTablet();
    void RegisterMethod_BuildRawDoc();

private:
    httplib::Server _httpServer;
    autil::ThreadPtr _listenThread;
    volatile int _port;
    volatile bool _hasError;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
