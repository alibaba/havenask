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
#include <assert.h>
#include <signal.h>

#include "navi/log/Thread.h"


static void *thread_routine(void *pArg)
{
    // Following code will guarantee more predictable latencies as it'll
    // disallow any signal handling in the I/O thread.
    sigset_t signal_set;
    int rc = sigfillset(&signal_set);
    assert(rc == 0);
    rc = pthread_sigmask(SIG_BLOCK, &signal_set, NULL);
    assert(rc == 0);

    navi::Thread *self = (navi::Thread *)pArg;
    self->m_tfn(self->m_pArg);
    (void)rc;

    return NULL;
}

void navi::Thread::start(thread_fn *tfn, void *pArg, const std::string &name)
{
    m_tfn = tfn;
    m_pArg = pArg;
    int rc = pthread_create(&m_threadDescriptor, NULL, thread_routine, this);
    assert(rc == 0);
    (void)(rc);
    if (!name.empty()) {
        pthread_setname_np(m_threadDescriptor, name.c_str());
    }
}

void navi::Thread::stop()
{
    int rc = pthread_join(m_threadDescriptor, NULL);
    assert(rc == 0);
    (void)(rc);
}
