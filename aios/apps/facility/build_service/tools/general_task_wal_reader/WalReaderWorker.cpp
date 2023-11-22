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
#include "tools/general_task_wal_reader/WalReaderWorker.h"

#include <cstdio>
#include <errno.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <readline/history.h>
#include <readline/readline.h>
#include <sstream>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "absl/flags/flag.h"
#include "absl/strings/str_join.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/wal/Wal.h"

// work mode related flags.
ABSL_FLAG(std::string, path, "", "path to wal directory");
ABSL_FLAG(bool, printRun, true, "print run log");

namespace build_service { namespace tools {

bool WalReaderWorker::run()
{
    std::string walPath = absl::GetFlag(FLAGS_path);
    if (walPath.empty()) {
        std::cerr << "wal path not specified, see --helpfull" << std::endl;
        return false;
    }
    bool printRunLog = absl::GetFlag(FLAGS_printRun);

    indexlib::file_system::WAL::WALOption walOption;
    walOption.workDir = walPath;
    walOption.isCheckSum = false;

    indexlib::file_system::WAL wal(walOption);
    if (!wal.Init()) {
        std::cerr << "init wal on " << walPath << " failed";
        return false;
    }
    while (true) {
        std::string recordStr;
        auto r = wal.ReadRecord(recordStr);
        if (!r) {
            std::cerr << "read wal failed" << std::endl;
            return false;
        }
        if (recordStr.empty()) {
            if (wal.IsRecovered()) {
                std::cout << "DONE" << std::endl;
                break;
            }
            continue;
        }
        proto::GeneralTaskWalRecord record;
        if (!record.ParseFromString(recordStr)) {
            std::cerr << "read wal record failed" << std::endl;
            return false;
        }
        printWalRecord(record, printRunLog);
    }
    std::cout << "finishedop: " << absl::StrJoin(_finishedOp, ",") << std::endl;
    std::vector<int64_t> runningOp;
    std::set_difference(_runOp.begin(), _runOp.end(), _finishedOp.begin(), _finishedOp.end(),
                        std::inserter(runningOp, runningOp.begin()));
    std::cout << "runingop: " << absl::StrJoin(runningOp, ",") << std::endl;
    return true;
}

void WalReaderWorker::printWalRecord(const proto::GeneralTaskWalRecord& record, bool printRunLog)
{
    ::printf("parallel[%u]\n", record.parallelnum());
    for (const auto& op : record.opfinish()) {
        ::printf("finish: op[%ld] on worker[%s], resultInfo[%s]\n", op.opid(), op.workerepochid().c_str(),
                 op.resultinfo().c_str());
        _finishedOp.insert(op.opid());
    }
    for (const auto& op : record.oprun()) {
        _runOp.insert(op.opid());
    }
    if (printRunLog) {
        for (const auto& op : record.oprun()) {
            ::printf("run: op[%ld] on node[%u]\n", op.opid(), op.nodeid());
        }
    }
    ::printf("######################################\n");
}

}} // namespace build_service::tools
