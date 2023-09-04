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
#ifndef HIPPO_TERMINATENOTIFIER_H
#define HIPPO_TERMINATENOTIFIER_H

#include "common/common.h"
#include "google/protobuf/service.h"
#include "autil/Lock.h"
#include <string>

BEGIN_HIPPO_NAMESPACE(util);

class TerminateNotifier
{
public:
    typedef ::google::protobuf::Closure Closure;
    static const int EXITED = (1 << 16) + 1;
public:
    TerminateNotifier() : _count(0), _exitFlag(false)
                        ,_closure(NULL)
    {}
    /***
        dec must be called with inc in pair. 
        And dec must be called after inc.
     */
    int inc() {
        autil::ScopedLock lock(_cond);
        ++_count;
        return 0;
    }
    int dec() {
        Closure * closure = NULL;
        int ret = 0;
        // get closure out of the lock range
        // to prevent "Run" delete "this"
        {
            // ENTER LOCK RANGE
            autil::ScopedLock lock(_cond);
            --_count;
            assert(_count >= 0);
            if (_count == 0) {
                if (_closure) {
                    closure = _closure;
                    _closure = NULL;
                }
                ret = _cond.broadcast();
            }
            // LEAVE LOCK RANGE
        }
        
        if (closure) {
            closure->Run();
        }
        return ret;
    }
    /**
     return 0 if got notified, and
     return errno or EXITED
     ether onTerminate or wait can be called, but not together
     */
    int wait(int timeout = -1) {
        autil::ScopedLock lock(_cond);
        while (true) {
            if (_count == 0) {
                return 0;
            }

            if (_exitFlag) {
                return EXITED;
            }

            int ret = _cond.wait(timeout);
            if (ret != 0) {
                return ret;
            }
        }
    }
    int peek() {
        autil::ScopedLock lock(_cond);
        return _count;
    }
    /*
      After onTerminate, NO MORE inc and dec should be called.
      The closure will be call only once.
      The closure MUST be self deleted or deleted by outer caller.
      
      ether onTerminate or wait can be called, but not together
    */
    int onTerminate(Closure * closure) {
        {
            // ENTER LOCK RANGE
            autil::ScopedLock lock(_cond);
            if ( (_count == 0) && (closure) ) {
                // event happen, and the new closure is not NULL
                // new closure should be called right now
                _closure = NULL;
            }
            else {
                // event not happen yet, or NULL is set
                // simply save it
                _closure = closure;
                return 0;
            }
            // LEAVE LOCK RANGE
        }
        
        if (closure) {
            closure->Run();
        }
        return 0;
    }
private:
    std::atomic_int _count;
    volatile bool _exitFlag;
    autil::ThreadCond _cond;
    Closure * _closure;
};

TYPEDEF_PTR(TerminateNotifier);

END_HIPPO_NAMESPACE(util);

#endif //HIPPO_TERMINATENOTIFIER_H
