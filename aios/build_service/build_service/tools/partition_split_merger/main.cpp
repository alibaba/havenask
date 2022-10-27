#include <iostream>
#include "build_service/tools/partition_split_merger/PartitionSplitMerger.h"
#include "build_service/tools/partition_split_merger/PartitionSplitChecker.h"
#include "build_service/util/LogSetupGuard.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::tools;
using namespace build_service::util;

void usage() {
    cout << "Usage: bs_partition_split_merger [merge generationDir "
         << "buildPartIdxFrom buildPartCount globalPartCount partSplitNum buildParallelNum]" << endl
         << "Usage: bs_partition_split_merger [check configPath  clusterName]" << endl;
}

int main(int argc, char **argv) {
    LogSetupGuard logGuard;

    if (argc < 2) {
        usage();
        return -1;
    }
    string cmdType = string(argv[1]);
    if (cmdType == "check" && argc == 4) {
        PartitionSplitChecker::Param param;
        param.configPath = string(argv[2]);
        param.clusterName = string(argv[3]);

        PartitionSplitChecker checker;
        if (!checker.init(param)) {
            cout << "ERROR: Init PartitionSplitChecker fail!" << endl;
            return -1;
        }
        if (!checker.check()) {
            cout << "ERROR: PartitionSplitChecker check fail!" << endl;
            return -1;
        }
        return 0;
    } else if (cmdType == "merge" && argc == 8) {
        PartitionSplitMerger::Param param;
        param.generationDir = string(argv[2]);
        uint32_t value;
        if (!StringUtil::fromString(string(argv[3]), value)) {
            usage();
            return -1;
        }
        param.buildPartIdxFrom = value;

        if (!StringUtil::fromString(string(argv[4]), value)) {
            usage();
            return -1;
        }
        param.buildPartCount = value;

        if (!StringUtil::fromString(string(argv[5]), value)) {
            usage();
            return -1;
        }
        param.globalPartCount = value;

        if (!StringUtil::fromString(string(argv[6]), value)) {
            usage();
            return -1;
        }
        param.partSplitNum = value;

        if (!StringUtil::fromString(string(argv[7]), value)) {
            usage();
            return -1;
        }
        param.buildParallelNum = value;

        PartitionSplitMerger merger;
        if (!merger.init(param)) {
            cout << "ERROR: PartitionSplitMerger init fail!" << endl;
            return -1;
        }

        if (!merger.merge()) {
            cout << "ERROR: PartitionSplitMerger merge fail!" << endl;
            return -1;
        }
        return 0;
    } else {
        usage();
        return -1;
    }
}
