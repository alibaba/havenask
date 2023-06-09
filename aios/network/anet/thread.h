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
#ifndef ANET_THREAD_H_
#define ANET_THREAD_H_
#include <pthread.h>
#include <sched.h>
#include <stddef.h>

#include "aios/network/anet/runnable.h"

//#include "aios/network/anet/log.h"
namespace anet {
  class Runnable;

  class Thread {

  public:
    /**
     * 构造函数
     */
    Thread() {
      tid = 0;
    }

    /**
     * 起一个线程，开始运行
     */
    void start(Runnable *r, void *a) {
      runnable = r;
      args = a;
      pthread_create(&tid, NULL, Thread::hook, this);
    }

    void setName(const char* name) {
      pthread_setname_np(tid, name);
    }

    /**
     * 等待线程退出
     */
    void join() {
      if (tid) {
	pthread_join(tid, NULL);
      }
      tid = 0;
    }

    /**
     * 得到Runnable对象
     * 
     * @return Runnable
     */
    Runnable *getRunnable() {
      return runnable;
    }
    
    /**
     * set the priority of this thread
     * @param priority Note only process with root privilege can set
     * priority to value greater than 0.
     * @param policy scheduling policy. Valid values are SCHED_OTHER,
     * SCHED_FIFO, SCHED_RR. SCHED_OTHER only has one priority 0.
     * SCHED_FIFO and SCHED_RR have priorities ranges from 1 to 99.
     * @return return true if success. 
     **/
    bool setPriority(int priority, int policy = SCHED_OTHER) {
      struct sched_param schedparam;
      schedparam.sched_priority = priority;
      int ret = pthread_setschedparam(tid, policy, &schedparam);
      if(ret != 0) {
//	ANET_LOG(WARN,"setPriority failed! Thread (%d), Error No(%d)", tid,ret);
	return false;
      }
      return true;
    }

    /**
     * 得到回调参数
     * 
     * @return args
     */
    void *getArgs() {
      return args;
    }

    /**
     * 线程的回调函数
     * 
     */

    static void *hook(void *arg) {
      Thread *thread = (Thread*) arg;

      if (thread->getRunnable()) {
	thread->getRunnable()->run(thread, thread->getArgs());
      }

      return (void*) NULL;
    }

  private:

    pthread_t tid;
    Runnable *runnable;
    void *args;
  };

}

#endif /*THREAD_H_*/
