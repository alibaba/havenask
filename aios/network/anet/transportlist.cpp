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
#include "aios/network/anet/transportlist.h"

#include <ostream>
#include <stddef.h>

#include "aios/network/anet/debug.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/transport.h"

using namespace std;

namespace anet {

/* The variable to remember current transport count in list. */
static int TransportCount = 0;

#define INC_TRANSPORT() (TransportCount++)
#define DEC_TRANSPORT() (TransportCount--)
TransportListT &GetTransportList() {
    /* We rely on the fact that static data will be cleared to
     * NULL by the compiler. */
    static TransportListT TransportList;
    return TransportList;
}

ThreadMutex &GetTransportListMutex() {
    static ThreadMutex TransportListMutex;
    return TransportListMutex;
}

int getTransportCount() {
    ThreadMutex &TransportListMutex = GetTransportListMutex();
    TransportListMutex.lock();
    int rc = TransportCount;
    TransportListMutex.unlock();
    return rc;
}

bool addTransportToList(Transport *t) {
    bool added = false;
    int i = 0;
    TransportListT &TransportList = GetTransportList();
    ThreadMutex &TransportListMutex = GetTransportListMutex();

    /* We don't want to add duplicated items. */
    if (findTransportFromList(t) < 0) {
        TransportListMutex.lock();
        for (i = 0; i < MAX_TRANSPORTS; i++) {
            if (TransportList[i] == NULL) {
                TransportList[i] = t;
                INC_TRANSPORT();
                added = true;
                break;
            }
        }
        TransportListMutex.unlock();
        DBGASSERT(getTransportCount() <= MAX_TRANSPORTS);
        if (i == MAX_TRANSPORTS) {
            ANET_LOG(WARN, "Slot number limits to %d. No more slots for Transport: %p", MAX_TRANSPORTS, t);
        }
    }
    return added;
}

bool delTransportFromList(Transport *t) {
    bool deleted = false;
    int i = 0;

    TransportListT &TransportList = GetTransportList();
    ThreadMutex &TransportListMutex = GetTransportListMutex();

    TransportListMutex.lock();
    for (i = 0; i < MAX_TRANSPORTS; i++) {
        if (TransportList[i] == t) {
            TransportList[i] = NULL;
            DEC_TRANSPORT();
            deleted = true;
            break;
        }
    }
    TransportListMutex.unlock();
    DBGASSERT(getTransportCount() >= 0);

    return deleted;
}

int findTransportFromList(Transport *t) {
    int i = 0;
    int rc = -1;

    TransportListT &TransportList = GetTransportList();
    ThreadMutex &TransportListMutex = GetTransportListMutex();

    TransportListMutex.lock();
    for (i = 0; i < MAX_TRANSPORTS; i++) {
        if (TransportList[i] == t) {
            rc = i;
            break;
        }
    }
    TransportListMutex.unlock();

    return rc;
}

int dumpTransportList(ostringstream &buf) {
    int i = 0;
    TransportListT &TransportList = GetTransportList();
    ThreadMutex &TransportListMutex = GetTransportListMutex();

    int count = getTransportCount();
    buf << "===============================Dump of Transports===============================" << endl
        << "Total count of transports: " << count << endl;
    if (count == MAX_TRANSPORTS) {
        buf << "[Warning]: Transport count is up to limit(" << MAX_TRANSPORTS << ")." << endl
            << "           Dumped information maybe incomplete." << endl;
    }

    TransportListMutex.lock();
    for (i = 0; i < MAX_TRANSPORTS; i++) {
        if (TransportList[i] != NULL)
            TransportList[i]->dump(buf);
    }
    TransportListMutex.unlock();

    return 0;
}

} // namespace anet
