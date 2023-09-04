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
#include "aios/network/anet/admincmds.h"

#include <map>
#include <stdio.h>
#include <string.h>
#include <string>
#include <utility>

#include "aios/network/anet/debug.h"
#include "aios/network/anet/globalflags.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/stats.h"
#include "aios/network/anet/threadmutex.h"
#include "aios/network/anet/transportlist.h"

using namespace std;
namespace anet {

/* forward declaration for cmd table use. */
static int dump(char *params, ostringstream &buf);
static int set(char *params, ostringstream &buf);
static int test(char *params, ostringstream &buf);
static int help(ostringstream &buf, CMDTable &cmdTable);

/**************************cmd table definations*******************************/
int initAnetCmds(CMDTable &cmdTable) {
    int rc = 0;
    SubSysEntry *anetSys = cmdTable.GetOrAddSubSys(string("anet"));
    anetSys->usage = "anet";

    CmdEntry cmdEntry;
    string subsysName = "anet";

    string cmdName = "dump";
    cmdEntry.usage = "dump [stats|config|transports|all]\n"
                     "    stats:  dump the global counters\n"
                     "    config: dump ANET config\n"
                     "    transports: dump all transports information\n"
                     "    all: default value. all of above.\n";
    cmdEntry.cb = dump;
    if (cmdTable.AddCmd(subsysName, cmdName, cmdEntry)) {
        rc++;
    }

    cmdName = "set";
    cmdEntry.usage = "set [config] [value]\n"
                     "    set checksum [enable|disable]\n"
                     "    set conntimeout [2000000] (us)\n";
    cmdEntry.cb = set;
    if (cmdTable.AddCmd(subsysName, cmdName, cmdEntry)) {
        rc++;
    }

    cmdName = "test";
    cmdEntry.usage = "test\n"
                     "    return a test string. internal use only.\n";
    cmdEntry.cb = test;
    if (cmdTable.AddCmd(subsysName, cmdName, cmdEntry)) {
        rc++;
    }

    return rc;
};
/**************************cmd map maintenance*********************************/
int CMDTable::dump(string leadingspace, ostringstream &buf) {
    MutexGuard slock(&lock);
    SubSysMap::iterator it;
    for (it = sMap.begin(); it != sMap.end(); it++) {
        buf << "Sub sys: [" << (*it).second.usage << "]" << endl;
        CmdMap::iterator cit;
        for (cit = (*it).second.cmds.begin(); cit != (*it).second.cmds.end(); cit++) {
            buf << leadingspace << (*cit).second.usage << endl;
        }
    }
    return 0;
}

SubSysEntry *CMDTable::GetOrAddSubSys(string name) {
    MutexGuard slock(&lock);
    SubSysMap::iterator it = sMap.find(name);

    if (it == sMap.end()) {
        SubSysEntry subsys;
        sMap[name] = subsys;
        it = sMap.find(name);
        DBGASSERT(it != sMap.end());
    }

    return &((*it).second);
}

SubSysEntry *CMDTable::GetSubSys(string name) {
    MutexGuard slock(&lock);
    SubSysMap::iterator it = sMap.find(name);
    if (it != sMap.end())
        return &((*it).second);
    return NULL;
}

bool CMDTable::AddCmd(string subsysName, string cmdName, CmdEntry &cmd) {
    bool rc = false;
    SubSysEntry *subsys = GetOrAddSubSys(subsysName);

    lock.lock();
    CmdMap::iterator it = subsys->cmds.find(cmdName);
    if (it == subsys->cmds.end()) {
        subsys->cmds[cmdName] = cmd;
        rc = true;
    }
    lock.unlock();
    return rc;
}

bool CMDTable::AddCmd(string subsys, string cmdName, string cmdUsage, ADMIN_CB cb) {
    CmdEntry cmd;
    cmd.usage = cmdUsage;
    cmd.cb = cb;

    return AddCmd(subsys, cmdName, cmd);
}

int CMDTable::ExecuteCmd(char *cmd, ostringstream &outbuf) {
    if (cmd == NULL)
        return -1;

    string cmdStr(cmd);
    return ExecuteCmd(cmdStr, outbuf);
}

int CMDTable::ExecuteCmd(string &cmdStr, ostringstream &outbuf) {
    /* incoming cmd string should be like the following format:
     *    <subsysName> <cmd> <parameters> ...  */
    int rc = -1;

    /* parameter sanity check */
    if (cmdStr == "") {
        ANET_LOG(DEBUG, "AdminServer: NULL cmd str.");
        outbuf << "AdminServer: NULL cmd. \n";
        return rc;
    }

    char cmd[MAX_CMD_LEN];
    snprintf(cmd, MAX_CMD_LEN, "%s", cmdStr.c_str());
    cmd[MAX_CMD_LEN - 1] = '\0';

    char *p = cmd;
    while (*p != '\0' && *p == ' ')
        p++;
    if (*p == '\0') {
        ANET_LOG(DEBUG, "AdminServer: empty cmd str.");
        outbuf << "AdminServer: empty cmd. \n";
        return rc;
    }

    char *subsys = p;
    /* extract subsys name */
    p = index(p, ' ');
    if (p != NULL) {
        *p++ = '\0';
        while (*p != '\0' && *p == ' ')
            p++;
    } else {
        /* no cmd. print help and exit. */
        outbuf << "AdminServer: Invalid input, cmd:" << cmdStr << endl;
        ANET_LOG(DEBUG, "AdminServer: Invalid cmd: %s", cmdStr.c_str());
        return rc;
    }

    char *cmdName = p;
    /* extract cmd and parameters and remove extra spaces. */
    char *param = index(p, ' ');
    if (param != NULL) {
        *param++ = '\0';
        while (*param != '\0' && *param == ' ')
            param++;
    }

    SubSysEntry *sys = GetSubSys(string(subsys));
    if (sys == NULL) {
        outbuf << "AdminServer: Invalid subsys name:" << subsys << ", cmd: " << cmdStr << endl;
        (void)help(outbuf, *this);
        return rc;
    }

    if (strncmp("help", subsys, 4) == 0 || strncmp("HELP", subsys, 4) == 0)
        return help(outbuf, *this);

    lock.lock();
    CmdMap::iterator it = sys->cmds.find(cmdName);
    lock.unlock();
    if (it == sys->cmds.end()) {
        outbuf << "AdminServer: Invalid cmd name:" << cmdName << ", for subsys: " << subsys << endl;
        (void)help(outbuf, *this);
        return rc;
    }

    CmdEntry &cmdEntry = (*it).second;

    rc = cmdEntry.cb(param, outbuf);
    if (rc) {
        ANET_LOG(DEBUG, "AdminServer: Invalid cmd. subsys: %s, cmd: %s, param: %s", subsys, cmdName, param);
        outbuf << "AdminServer: Invalid cmd:" << cmdStr << endl;
    }
    return rc;
}

/**************************cmd implementation********************************/
static int help(ostringstream &buf, CMDTable &cmdTable) {
    buf << "Usage: <cmd> <pid> <subsys name> <cmd name> <param...>" << endl;
    buf << "Supported Sub Sys and Cmds:" << endl;
    string leadspace("  ");
    return cmdTable.dump(leadspace, buf);
}

static int dump(char *params, ostringstream &buf) {
    int rc = 0;
    bool dumpall = false;
    /* If no params are available, params will be set to NULL. */
    char *p = params;

    /* If no parameter, assume "all" */
    if (p == NULL || *p == '\0')
        p = (char *)"all";

    if (strncmp(p, "all", 3) == 0)
        dumpall = true;

    if (strncmp(p, "config", 6) == 0 || dumpall) {
        rc = flags::dump(buf);
        buf << "--------------------------------------------------------------------------------" << endl;
    }
    if (strncmp(p, "stats", 5) == 0 || dumpall) {
        rc = StatCounter::_gStatCounter.dump(buf);
    }
    if (strncmp(p, "transports", 10) == 0 || dumpall) {
        rc = dumpTransportList(buf);
        buf << "--------------------------------------------------------------------------------" << endl;
    }

    ANET_LOG(SPAM, "AdminServer: dump cmd executed, params: %s", params);
    return rc;
}

static int set(char *params, ostringstream &buf) {
    int rc = -1;
    /* If no params are available, params will be set to NULL. */
    char *p = params;
    if (p == NULL || *p == '\0')
        return rc;

    if (strncmp(p, "checksum", 8) == 0) {
        p += 8;
        while (p != NULL && *p == ' ')
            p++;
        if (*p == '\0')
            return rc;

        if (strncmp(p, "enable", 6) == 0) {
            rc = 0;
            flags::setChecksumState(true);
        } else if (strncmp(p, "disable", 7) == 0) {
            rc = 0;
            flags::setChecksumState(false);
        } else {
            return rc;
        }
    } else if (strncmp(p, "conntimeout", 11) == 0) {
        p += 11;
        while (p != NULL && *p == ' ')
            p++;
        if (*p == '\0')
            return rc;
        int connTimeout;
        if (sscanf(p, "%d", &connTimeout) != 0) {
            rc = 0;
            flags::setConnectTimeout(connTimeout);
        }
    }

    ANET_LOG(SPAM, "AdminServer: set cmd executed, params: %s", params);
    return rc;
}

static int test(char *params, ostringstream &buf) {
    /* If no params are available, it will be set to NULL. */

    for (int i = 0; i < 2999; i++)
        buf << 'A';
    buf << '\0';
    ANET_LOG(SPAM, "AdminServer: test cmd executed, params: %s", params);
    return 3000;
}

} // namespace anet
