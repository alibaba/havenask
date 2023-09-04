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
#ifndef ISEARCH_TERMINATENOTIFIER_H
#define ISEARCH_TERMINATENOTIFIER_H

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"

namespace multi_call {

class Closure;

class TerminateNotifier
{
public:
    TerminateNotifier();
    ~TerminateNotifier();

private:
    TerminateNotifier(const TerminateNotifier &);
    void operator=(const TerminateNotifier &);

public:
    void inc(int partIdIndex);
    void dec(int partIdIndex);
    void setClosure(Closure *closure);
    Closure *stealClosure();
    bool canTerminate() const {
        return _partitionCount == 0;
    }

private:
    void incPartitionsByIndex(int index);
    void decPartitionsByIndex(int index);

private:
    volatile int _partitionCount;
    Closure *_closure;
    std::vector<int> _partitions;

    mutable autil::ThreadMutex _mutex;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_TERMINATENOTIFIER_H
