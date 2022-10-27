#include "build_service/util/LogSetupGuard.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/common_define.h"
#include <string>
#include <iostream>

using namespace std;
using namespace build_service;
using namespace build_service::util;
using build_service::util::LogSetupGuard;

static const string usage = "generation_scanner indexRoot clusterName";

int main(int argc, char **argv) {
    LogSetupGuard logGuard(LogSetupGuard::FILE_LOG_CONF);

    if (argc != 3) {
        cerr << usage << endl;
        return -1;
    }
    string indexRoot(argv[1]);
    string clusterName(argv[2]);

    vector<generationid_t> generations = IndexPathConstructor::getGenerationList(
            indexRoot, clusterName);
    if (generations.empty()) {
        cout << -1;
    } else {
        cout << generations.back();
    }
    return 0;
}
