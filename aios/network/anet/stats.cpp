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
#include "aios/network/anet/stats.h"

#include <ostream>
#include <stdint.h>

#include "aios/network/anet/atomic.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"

using namespace std;
namespace anet {

StatCounter StatCounter::_gStatCounter;

StatCounter::StatCounter() { clear(); }

StatCounter::~StatCounter() {}

void StatCounter::log() {
    ANET_LOG(INFO,
             "_packetReadCnt: %lld, _packetWriteCnt: %lld, "
             "_packetTimeoutCnt: %lld, _dataReadCnt: %lld, "
             "_dataWriteCnt: %lld, _inputBufferSpaceAllocated: %lld, "
             "_outputBufferSpaceAllocated: %lld, "
             "_outputBufferSpaceUsed: %lld, _outputQueueSpaceUsed: %lld, _outputQueueSize: %lld",
             atomic_read(&_packetReadCnt),
             atomic_read(&_packetWriteCnt),
             atomic_read(&_packetTimeoutCnt),
             atomic_read(&_dataReadCnt),
             atomic_read(&_dataWriteCnt),
             atomic_read(&_inputBufferSpaceAllocated),
             atomic_read(&_outputBufferSpaceAllocated),
             atomic_read(&_outputBufferSpaceUsed),
             atomic_read(&_outputQueueSpaceUsed),
             atomic_read(&_outputQueueSize));
}

int StatCounter::dump(ostringstream &buf) {
    buf << "===============================Global Counters==================================\n";
    buf << "Network Statistics:\n";
    buf << "packet read count: " << atomic_read(&_packetReadCnt);
    buf << "\tpacket write count: " << atomic_read(&_packetWriteCnt);
    buf << "\tpacket timeout count: " << atomic_read(&_packetTimeoutCnt) << endl;
    buf << "bytes read : " << atomic_read(&_dataReadCnt);
    buf << "\tbytes write: " << atomic_read(&_dataWriteCnt) << endl << endl;

    buf << "Memory Statistics:\n";
    buf << "total input buffer bytes allocated: " << atomic_read(&_inputBufferSpaceAllocated) << endl;
    buf << "total output buffer bytes allocated: " << atomic_read(&_outputBufferSpaceAllocated) << endl;
    buf << "total output buffer bytes used: " << atomic_read(&_outputBufferSpaceUsed) << endl;
    buf << "total output queue bytes used: " << atomic_read(&_outputQueueSpaceUsed) << endl << endl;
    buf << "total output queue size: " << atomic_read(&_outputQueueSize) << endl << endl;
    return 0;
}

void StatCounter::clear() {
    atomic_set(&_packetReadCnt, 0);
    atomic_set(&_packetWriteCnt, 0);
    atomic_set(&_dataReadCnt, 0);
    atomic_set(&_dataWriteCnt, 0);
    atomic_set(&_packetTimeoutCnt, 0);
    atomic_set(&_inputBufferSpaceAllocated, 0);
    atomic_set(&_outputBufferSpaceAllocated, 0);
    atomic_set(&_outputBufferSpaceUsed, 0);
    atomic_set(&_outputQueueSpaceUsed, 0);
    atomic_set(&_outputQueueSize, 0);
}

int64_t StatCounter::getPacketReadCnt() { return atomic_read(&_packetReadCnt); }

int64_t StatCounter::getPacketWriteCnt() { return atomic_read(&_packetWriteCnt); }

int64_t StatCounter::getPacketTimeoutCnt() { return atomic_read(&_packetTimeoutCnt); }

int64_t StatCounter::getDataReadCnt() { return atomic_read(&_dataReadCnt); }

int64_t StatCounter::getDataWriteCnt() { return atomic_read(&_dataWriteCnt); }

int64_t StatCounter::getInputBufferSpaceAllocated() { return atomic_read(&_inputBufferSpaceAllocated); }

int64_t StatCounter::getOutputBufferSpaceAllocated() { return atomic_read(&_outputBufferSpaceAllocated); }

int64_t StatCounter::getOutputBufferSpaceUsed() { return atomic_read(&_outputBufferSpaceUsed); }

int64_t StatCounter::getOutputQueueSpaceUsed() { return atomic_read(&_outputQueueSpaceUsed); }

int64_t StatCounter::getOutputQueueSize() { return atomic_read(&_outputQueueSize); }

} // namespace anet
