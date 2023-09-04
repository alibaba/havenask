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
#ifndef ADMIN_CMDS_H_
#define ADMIN_CMDS_H_

#include <iostream>
#include <map>
#include <string>

#include "aios/network/anet/threadmutex.h"

namespace anet {

/* Defines the maximum length for the command passed in from user side. */
#define MAX_CMD_LEN 256
/* Defines the maximum path length to hold domain socket path. */
#define MAX_PATH_LEN 256
/* Defines the maximum buffer length when getting result from AdminServer. */
#define CMD_BUF_SIZE 1 * 1024 * 1024

enum {
    E_OK = 0,
    E_FAILED = 1
};

/*************************command table definations***************************/
/* Redefine the type for CmdTable. */
typedef int (*ADMIN_CB)(char *params, std::ostringstream &buf);
/*---------------------------------------------------------------------------*/
struct CmdEntry {
    std::string usage;
    ADMIN_CB cb;
};
/* Cmd name, Cmd Entry */
typedef std::map<std::string, CmdEntry> CmdMap;

struct SubSysEntry {
    std::string usage;
    CmdMap cmds;
};
/* Sub sys name, cmds */
typedef std::map<std::string, SubSysEntry> SubSysMap;

class CMDTable {
public:
    SubSysEntry *GetOrAddSubSys(std::string name);
    SubSysEntry *GetSubSys(std::string name);
    bool AddCmd(std::string subsys, std::string cmdName, std::string cmdUsage, ADMIN_CB cb);
    bool AddCmd(std::string subsys, std::string cmdName, CmdEntry &cmd);
    CmdEntry &GetCmd(std::string subsys, std::string cmd);

    int ExecuteCmd(std::string &cmdStr, std::ostringstream &outbuf);
    int ExecuteCmd(char *cmdStr, std::ostringstream &outbuf);

    int dump(std::string leadingspace, std::ostringstream &buf);

private:
    SubSysMap sMap;
    ThreadMutex lock;
};

int initAnetCmds(CMDTable &cmdTable);

} // namespace anet
#endif
