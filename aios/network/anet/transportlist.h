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
#ifndef TRANSPORT_LIST_H_
#define TRANSPORT_LIST_H_

#include <sstream>

#include "aios/network/anet/threadmutex.h"

namespace anet {

class Transport;

/**
 * Global list is maintained for all transport objects in a process.
 * In Transport constructor and destructor, the object will be linked to or
 * removed from the global transport list. 
 * The purpose of this list is to provide an entry point so that the connection
 * state can get accessed. 
 */

/* We assume that in a process, there will be no more than 1024 transports 
 * get used in the same time. This assumption may become invalid one day and we
 * just need to increase the value then. */
#define MAX_TRANSPORTS 1024

/* Redefine the transport pointer array. */
typedef Transport* TransportListT[MAX_TRANSPORTS];

/* Group of helper functions to manipulate the operation of transport list. */
TransportListT & GetTransportList();
ThreadMutex & GetTransportListMutex();

bool addTransportToList(Transport *t);
bool delTransportFromList(Transport *t);
int  findTransportFromList(Transport *t);

int getTransportCount(void);

/* Dump the content of all the active transports in the system. */
int dumpTransportList(std::ostringstream &buf);

}
#endif
