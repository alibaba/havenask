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
#ifndef ISEARCH_BS_TCMALLOCMEMCONTROLLER_H
#define ISEARCH_BS_TCMALLOCMEMCONTROLLER_H

#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class TCMallocMemController
{
public:
    TCMallocMemController();
    ~TCMallocMemController();

private:
    TCMallocMemController(const TCMallocMemController&);
    TCMallocMemController& operator=(const TCMallocMemController&);

public:
    bool start();

private:
    void printMem() const;
    void releaseMem() const;

private:
    static const int64_t DEFAULT_MEM_PRINTINT_INTERVAL;
    static const int64_t DEFAULT_MEM_RELEASE_INTERVAL;

private:
    int64_t _memPrintingInterval;
    int64_t _memReleaseInterval;
    autil::LoopThreadPtr _memPrinterThread;
    autil::LoopThreadPtr _memReleaseThread;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TCMallocMemController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_TCMALLOCMEMCONTROLLER_H
