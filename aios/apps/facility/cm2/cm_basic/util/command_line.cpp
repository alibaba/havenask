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
#include "aios/apps/facility/cm2/cm_basic/util/command_line.h"

#include <ctype.h>
#include <errno.h>
#include <linux/limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "aios/apps/facility/cm2/cm_basic/util/file_util.h"
#include "aios/apps/facility/cm2/cm_basic/util/server.h"
#include "autil/Lock.h"

#define pidFileTMPL "/tmp/.%s_%s_pidfile"

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, CommandLine);

typedef enum { cmd_start, cmd_stop, cmd_restart, cmd_unknown } cmd_type_t;

class Args
{
public:
    Args(int argc, char* argv[]);
    ~Args() {}

public:
    const char* getLogConfPath() const { return _logPath; }
    const char* getConfPath() const { return _confPath; }
    bool isForHelp() const { return _help; }
    bool isForShortVersion() const { return _verShort; }
    bool isForLongVersion() const { return _verLong; }
    bool asDaemon() const { return _daemon; }
    cmd_type_t getCommandType() const { return _cmd; }

private:
    char _logPath[PATH_MAX + 1];
    char _confPath[PATH_MAX + 1];
    bool _verShort;
    bool _verLong;
    bool _help;
    bool _daemon;
    cmd_type_t _cmd;
};

Args::Args(int argc, char* argv[]) : _verShort(false), _verLong(false), _help(false), _daemon(false), _cmd(cmd_start)
{
    int c = 0;
    _logPath[0] = '\0';
    _confPath[0] = '\0';
    if (argv == NULL) {
        return;
    }
    while ((c = getopt(argc, argv, "k:c:dl:vVh")) != -1) {
        switch (c) {
        case 'c':
            strcpy(_confPath, optarg);
            break;
        case 'l':
            strcpy(_logPath, optarg);
            break;
        case 'k':
            if (strcmp(optarg, "restart") == 0) {
                _cmd = cmd_restart;
            } else if (strcmp(optarg, "start") == 0) {
                _cmd = cmd_start;
            } else if (strcmp(optarg, "stop") == 0) {
                _cmd = cmd_stop;
            } else {
                _cmd = cmd_unknown;
            }
            break;
        case 'd':
            _daemon = true;
            break;
        case 'v':
            _verShort = true;
            break;
        case 'V':
            _verLong = true;
            break;
        case 'h':
            _help = true;
            break;
        case '?':
            if (optopt != 'c' && optopt != 'l' && isprint(optopt)) {
                fprintf(stderr, "Unknown option `-%c'.\n", optopt);
            } else {
                fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
            }
            break;
        default:
            abort();
        }
    }
    if (_daemon) {
        // TODO: convert to abs path
    }
}

pid_t getPidFromFile(const char* pidfile)
{
    long p = 0;
    pid_t pid;
    FILE* file = NULL;
    /* make sure this program isn't already running. */
    file = fopen(pidfile, "r");
    if (!file) {
        return -1;
    }
    if (fscanf(file, "%ld", &p) == 1 && (pid = p) && (getpgid(pid) > -1)) {
        fclose(file);
        return pid;
    }
    fclose(file);
    return 0;
}

int updatePidFile(const char* pidfile)
{
    long p = 0;
    pid_t pid;
    mode_t prev_umask;
    FILE* file = NULL;
    /* make sure this program isn't already running. */
    file = fopen(pidfile, "r");
    if (file) {
        if (fscanf(file, "%ld", &p) == 1 && (pid = p) && (getpgid(pid) > -1)) {
            fclose(file);
        }
    }
    /* write the pid of this process to the pidfile */
    prev_umask = umask(0112);
    unlink(pidfile);
    file = fopen(pidfile, "w");
    if (!file) {
        fprintf(stderr, "Error writing pidfile '%s' -- %s\n", pidfile, strerror(errno));
        return -1;
    }
    fprintf(file, "%ld\n", (long)getpid());
    fclose(file);
    umask(prev_umask);
    return 0;
}

CommandLine::CommandLine(const char* name) : _name(NULL), _server(NULL), _next(NULL)
{
    if (name) {
        _name = strdup(name);
    } else {
        _name = strdup("unknown-server");
    }
}

CommandLine::~CommandLine()
{
    if (_name) {
        ::free(_name);
    }
    if (_server) {
        delete _server;
        _server = NULL;
    }
}

static autil::ThreadMutex G_CMD_LOCK;
static CommandLine* G_CMD_LIST = NULL;

int CommandLine::run(int argc, char* argv[])
{
    bool ret = false;
    bool needWait = false;
    char pidFile[PATH_MAX + 1] = {0};
    char confName[PATH_MAX + 1] = {0};
    pid_t curPid = 0;
    Args args(argc, argv);
    char* pconf = cm_basic::FileUtil::getRealPath(args.getConfPath(), confName);
    if (_server) {
        fprintf(stderr, "%s is running.\n", _name);
        return -1;
    }
    if (args.isForHelp()) {
        help();
        return 0;
    } else if (args.isForLongVersion()) {
        showLongVersion();
        return 0;
    } else if (args.isForShortVersion()) {
        showShortVersion();
        return 0;
    }

    if (setSigmask() == false)
        return -1;

    for (char* pTmp = pconf; *pTmp; pTmp++) {
        if (*pTmp == '/') {
            *pTmp = '_';
        }
    }
    snprintf(pidFile, strlen(_name) + strlen(pidFileTMPL) + strlen(pconf) + 1, pidFileTMPL, _name, pconf);
    curPid = getPidFromFile(pidFile);
    if (args.getCommandType() == cmd_unknown || strlen(args.getConfPath()) == 0) {
        help(-1);
        return -1;
    } else if (args.getCommandType() == cmd_stop) {
        if (curPid > 0) {
            if (kill(curPid, SIGTERM) != 0) {
                fprintf(stderr, "kill process error, pid = %ld\n", (int64_t)curPid);
            }
        } else {
            fprintf(stderr, "process not running.\n");
        }
        return -1;
    } else if (args.getCommandType() == cmd_restart) {
        if (curPid > 0) {
            if (kill(curPid, SIGTERM) != 0) {
                fprintf(stderr, "kill process error, pid = %ld\n", (int64_t)curPid);
                return -1;
            }
            needWait = true;
        } else {
            fprintf(stderr, "process not running.\n");
        }
    } else if (args.getCommandType() == cmd_start) {
        if (curPid > 0) {
            fprintf(stderr,
                    "process already running."
                    "stop it first. pid=%ld, pidfile=%s\n",
                    (int64_t)curPid, pidFile);
            return -1;
        }
    }

    if (args.asDaemon()) {
        if (daemon(1, 1) == -1) {
            fprintf(stderr, "set self process as daemon failed.\n");
        }
    }
    if ((args.getCommandType() == cmd_restart) && (needWait)) {
        int errnum = 0;
        while (getPidFromFile(pidFile) >= 0) {
            if (errnum > 3000) {
                fprintf(stderr, "shutdown previous server failed: too slow\n");
                alog::Logger::shutdown();
                return -1;
            }
            usleep(20);
            errnum++;
        }
    }
    if (updatePidFile(pidFile) < 0) {
        fprintf(stderr, "write pid to file %s error\n", pidFile);
        alog::Logger::shutdown();
        return -1;
    }
    if (strlen(args.getLogConfPath()) > 0) {
        try {
            alog::Configurator::configureLogger(args.getLogConfPath());
        } catch (std::exception& e) {
            fprintf(stderr, "config from '%s' failed, using default root.\n", args.getLogConfPath());
            alog::Configurator::configureRootLogger();
        }
    } else {
        alog::Configurator::configureRootLogger();
    }
    AUTIL_LOG(INFO, "server begin to start: pid=%ld", (int64_t)getpid());

    _server = makeServer();
    if (!_server) {
        AUTIL_LOG(ERROR, "create server failed.\n");
        alog::Logger::shutdown();
        return -1;
    }

    ret = _server->start(args.getConfPath());
    if (ret != true) {
        AUTIL_LOG(ERROR, "startting server by `%s' failed.\n", args.getConfPath());
        delete _server;
        _server = NULL;
        alog::Logger::shutdown();
        unlink(pidFile);
        return -2;
    }
    AUTIL_LOG(INFO, "server start success! pid=%ld", (int64_t)getpid());

    // 添加
    {
        autil::ScopedLock lock(G_CMD_LOCK);
        _next = G_CMD_LIST;
        G_CMD_LIST = this;
    }

    waitSig();

    _server->wait();
    delete _server;
    _server = NULL;

    alog::Logger::shutdown();
    unlink(pidFile);

    autil::ScopedLock lock(G_CMD_LOCK);
    if (G_CMD_LIST == this) {
        G_CMD_LIST = _next;
    } else {
        for (CommandLine* prev = G_CMD_LIST; prev; prev = prev->_next) {
            if (prev->_next == this) {
                prev->_next = _next;
                break;
            }
        }
    }
    return 0;
}

void CommandLine::terminate()
{
    if (_server) {
        _server->stop();
    }
}

void CommandLine::help(int type)
{
    FILE* fp = NULL;
    if (type == 0) {
        fp = stdout;
    } else {
        fp = stderr;
    }
    fprintf(fp, "Usage: %s -c <server_confPath> -k start|stop|restart [-l <log_confPath>] [-d]\n", _name);
    fprintf(fp, "             [-v] [-V] [-h]\n");
    fprintf(fp, "Options:\n");
    fprintf(fp, "  -c <server_confPath>    : specify an alternate ServerConfigFile\n");
    fprintf(fp, "  -l <log_confPath>       : specify an alternate log serup conf file\n");
    fprintf(fp, "  -k start|stop|restart    : specify an alternate server operator\n");
    fprintf(fp, "  -d                       : set self process as daemon\n");
    fprintf(fp, "  -v                       : show version number\n");
    fprintf(fp, "  -V                       : show compile settings\n");
    fprintf(fp, "  -h                       : list available command line options (this page)\n");
    fprintf(fp, "Example:\n");
    fprintf(fp, "  %s -c server.cfg -k restart -l log.cfg -d\n", _name);
}

void CommandLine::showShortVersion() { help(); }

void CommandLine::showLongVersion() { help(); }

static void handleServer(int sig)
{
    autil::ScopedLock lock(G_CMD_LOCK);
    if (sig == SIGUSR1 || sig == SIGUSR2 || sig == SIGINT || sig == SIGTERM) {
        for (CommandLine* p = G_CMD_LIST; p; p = p->_next) {
            p->terminate();
        }
    }
}

void CommandLine::registerSig()
{
    signal(SIGPIPE, SIG_IGN);
    signal(SIGUSR1, handleServer);
    signal(SIGUSR2, handleServer);
    signal(SIGINT, handleServer);
    signal(SIGTERM, handleServer);
}

bool CommandLine::setSigmask()
{
    int err = 0;
    sigset_t oldMask;
    sigemptyset(&_mask);
    sigaddset(&_mask, SIGINT);
    sigaddset(&_mask, SIGTERM);
    sigaddset(&_mask, SIGUSR1);
    sigaddset(&_mask, SIGUSR2);
    sigaddset(&_mask, SIGPIPE);
    if ((err = pthread_sigmask(SIG_BLOCK, &_mask, &oldMask)) != 0) {
        fprintf(stderr, "Set sigmask error: app=%s.\n", _name);
        return false;
    }
    return true;
}

void CommandLine::waitSig()
{
    int err = 0;
    int signo = 0;
    while (1) {
        err = sigwait(&_mask, &signo);
        if (err != 0) {
            continue;
        }
        switch (signo) {
        case SIGUSR1:
        case SIGUSR2:
        case SIGINT:
        case SIGTERM:
            AUTIL_LOG(INFO, "server receive signal=%d, start to quit!", signo);
            {
                autil::ScopedLock lock(G_CMD_LOCK);
                for (CommandLine* p = G_CMD_LIST; p; p = p->_next) {
                    p->terminate();
                }
            }
            return;
        default:
            continue;
        }
    } // while
}

} // namespace cm_basic
