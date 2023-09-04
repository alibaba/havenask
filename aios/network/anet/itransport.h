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
#ifndef ITRANSPORT_H
#define ITRANSPORT_H

#include <cstdlib>

namespace anet {
/* This class defines the interface of the Reactor/Proactor process
 * such as Transport in anet, io_service in asio and event_base in libevent.
 */
class ITransport {
public:
    /* This function will blocks the caller thread and checking forever if
     * any new event comes in the monitored handles.
     * @ret - the handlers being invoked during run().
     */
    virtual void run() = 0;

    /* Run the event loop for one time and then exit
     * @ret - the handlers being invoked during runOnce().
     */
    virtual void runOnce() = 0;

    /* Stop the event loop and all the framework threads.
     * All the threads that blocking in run() and runOnce() should get
     * returned immediately.
     * Note: this function won't wait for all the threads to exit, instead
     * it just send signals to all the threads and then return.
     */
    virtual bool stop() = 0;

    /* Reset the event processing framework to make it ready for 2nd run(),
     * runOnce() after Stop() is invoked.
     */
    virtual void reset() = 0;

    /*
     * This function will start a thread to check forever if
     * any new event comes in the monitored handles.
     */
    virtual void runInThread() = 0;

    virtual ~ITransport() {}
};

} // namespace anet

#endif
