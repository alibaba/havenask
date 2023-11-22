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
#include <fstream>
#include <iostream>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/flags/usage.h"
#include "fslib/fslib.h"
#include "general_task_wal_reader/WalReaderWorker.h"
// example usage:
// ./wal-reader --path walDirectory

int main(int argc, char** argv)
{
    absl::SetProgramUsageMessage("wal decoder");
    absl::ParseCommandLine(argc, argv);
    build_service::tools::WalReaderWorker worker;
    auto succ = worker.run();
    fslib::fs::FileSystem::close();
    return succ ? 0 : -1;
}
