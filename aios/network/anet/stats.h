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
#ifndef ANET_STATS_H_
#define ANET_STATS_H_
#include <stdint.h>
#include <iostream>
#include <sstream>

#include "aios/network/anet/atomic.h"


namespace anet {

class StatCounter
{
public:
    StatCounter();
    ~StatCounter();
    void log();
    void clear();
    int64_t getPacketReadCnt();
    int64_t getPacketWriteCnt();
    int64_t getPacketTimeoutCnt();
    int64_t getDataReadCnt();
    int64_t getDataWriteCnt();
    int64_t getInputBufferSpaceAllocated();
    int64_t getOutputBufferSpaceAllocated();
    int64_t getOutputBufferSpaceUsed();
    int64_t getOutputQueueSpaceUsed();

    int dump(std::ostringstream &buf);
    
    int64_t getOutputQueueSize();
public:    
    atomic64_t _packetReadCnt;    // packets read
    atomic64_t _packetWriteCnt;   // packets written
    atomic64_t _packetTimeoutCnt; // packets timeout
    atomic64_t _dataReadCnt;      // bytes read
    atomic64_t _dataWriteCnt;     // bytes written
   
    // SpaceUsed is for true data size, SpaceAllocated means the total buffer size
    atomic64_t _inputBufferSpaceAllocated;
    atomic64_t _outputBufferSpaceAllocated;
    atomic64_t _outputBufferSpaceUsed;
    atomic64_t _outputQueueSpaceUsed;
    atomic64_t _outputQueueSize;
public:
    static StatCounter _gStatCounter;
};

#define ANET_GLOBAL_STAT StatCounter::_gStatCounter
#define ANET_COUNT_PACKET_READ(i) {atomic_add((i), &(ANET_GLOBAL_STAT._packetReadCnt));}
#define ANET_COUNT_PACKET_WRITE(i) {atomic_add((i), &(ANET_GLOBAL_STAT._packetWriteCnt));}
#define ANET_COUNT_PACKET_TIMEOUT(i) {atomic_add((i), &(ANET_GLOBAL_STAT._packetTimeoutCnt));}
#define ANET_COUNT_DATA_READ(i) {atomic_add((i), &(ANET_GLOBAL_STAT._dataReadCnt));}
#define ANET_COUNT_DATA_WRITE(i) {atomic_add((i), &(ANET_GLOBAL_STAT._dataWriteCnt));}

#define ANET_ADD_INPUT_BUFFER_SPACE_ALLOCATED(i) {atomic_add((i), &(ANET_GLOBAL_STAT._inputBufferSpaceAllocated));}
#define ANET_ADD_OUTPUT_BUFFER_SPACE_ALLOCATED(i) {atomic_add((i), &(ANET_GLOBAL_STAT._outputBufferSpaceAllocated));}
#define ANET_ADD_OUTPUT_BUFFER_SPACE_USED(i) {atomic_add((i), &(ANET_GLOBAL_STAT._outputBufferSpaceUsed));}
#define ANET_ADD_OUTPUT_QUEUE_SPACE_USED(i) {atomic_add((i), &(ANET_GLOBAL_STAT._outputQueueSpaceUsed));}
#define ANET_ADD_OUTPUT_QUEUE_SIZE(i) {atomic_add((i), &(ANET_GLOBAL_STAT._outputQueueSize));}

}

#endif

