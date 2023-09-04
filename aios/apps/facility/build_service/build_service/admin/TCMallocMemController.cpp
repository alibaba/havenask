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
#include "build_service/admin/TCMallocMemController.h"

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <malloc.h>
#include <time.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, TCMallocMemController);

const int64_t TCMallocMemController::DEFAULT_MEM_PRINTINT_INTERVAL = 600;
const int64_t TCMallocMemController::DEFAULT_MEM_RELEASE_INTERVAL = 3600 * 3;

TCMallocMemController::TCMallocMemController()
    : _memPrintingInterval(DEFAULT_MEM_PRINTINT_INTERVAL)
    , _memReleaseInterval(DEFAULT_MEM_RELEASE_INTERVAL)
{
}

TCMallocMemController::~TCMallocMemController() {}

bool TCMallocMemController::start()
{
    {
        string param = autil::EnvUtil::getEnv(BS_ENV_MEM_PRINTING_INTERVAL.c_str());
        int64_t interval = 0;
        if (!param.empty() && StringUtil::fromString(param, interval) && interval > 0) {
            _memPrintingInterval = interval;
        }
        BS_LOG(INFO, "memory printing interval is %ld seconds", _memPrintingInterval);
    }
    {
        string param = autil::EnvUtil::getEnv(BS_ENV_MEM_RELEASE_INTERVAL.c_str());
        int64_t interval = 0;
        if (!param.empty() && StringUtil::fromString(param, interval) && interval > 0) {
            _memReleaseInterval = interval;
        }
        BS_LOG(INFO, "memory release interval is %ld seconds", _memReleaseInterval);
    }
    _memPrinterThread =
        LoopThread::createLoopThread(bind(&TCMallocMemController::printMem, this), _memPrintingInterval * 1000000);
    if (!_memPrinterThread) {
        return false;
    }
    _memReleaseThread =
        LoopThread::createLoopThread(bind(&TCMallocMemController::releaseMem, this), _memReleaseInterval * 1000000);
    if (!_memReleaseThread) {
        return false;
    }
    return true;
}

void TCMallocMemController::printMem() const
{
    char buf[30];
    time_t clock;
    time(&clock);

    struct tm* tm = localtime(&clock);
    strftime(buf, sizeof(buf), " %x %X %Y ", tm);

    cout << "########" << buf << "########" << endl;
    cerr << "########" << buf << "########" << endl;
    malloc_stats();

    struct mallinfo mi;
    mi = mallinfo();
    cerr << "Total non-mmapped bytes (arena):       " << mi.arena << endl;
    cerr << "# of free chunks (ordblks):            " << mi.ordblks << endl;
    cerr << "# of free fastbin blocks (smblks):     " << mi.smblks << endl;
    cerr << "# of mapped regions (hblks):           " << mi.hblks << endl;
    cerr << "Bytes in mapped regions (hblkhd):      " << mi.hblkhd << endl;
    cerr << "Max. total allocated space (usmblks):  " << mi.usmblks << endl;
    cerr << "Free bytes held in fastbins (fsmblks): " << mi.fsmblks << endl;
    cerr << "Total allocated space (uordblks):      " << mi.uordblks << endl;
    cerr << "Total free space (fordblks):           " << mi.fordblks << endl;
    cerr << "Topmost releasable block (keepcost):   " << mi.keepcost << endl;
}

void TCMallocMemController::releaseMem() const
{
    BS_LOG(INFO, "release free memory");
    MallocExtension::instance()->ReleaseFreeMemory();
}

}} // namespace build_service::admin
