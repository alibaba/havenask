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
#ifndef __COMMAND_LINE_H_
#define __COMMAND_LINE_H_
#include <signal.h>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace cm_basic {

class Args;
class Server;

class CommandLine
{
public:
    CommandLine(const char* name);
    virtual ~CommandLine();

public:
    int run(int argc, char* argv[]);

    void terminate();

    bool setSigmask();

    void waitSig();

    static void registerSig();

protected:
    virtual Server* makeServer() = 0;

private:
    void help(int type = 0);
    void showShortVersion();
    void showLongVersion();

protected:
    char* _name;
    sigset_t _mask;
    Server* _server;
    autil::ThreadCond _lock;

public:
    CommandLine* _next;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic

#endif
