#include <benchmark/benchmark.h>
#include <unistd.h>
#include <limits.h>
#include "autil/Log.h"

// BENCHMARK_MAIN
int main(int argc, char** argv) {
    std::string cwd;
    std::string test_binary = argv[0];
    {
        char buf[PATH_MAX];
        if(getcwd(buf, PATH_MAX)==nullptr) {
            return -1;
        }
        cwd = buf;
    }
    std::cout << "CWD: " << cwd << std::endl;
    std::cout << "TEST_BINARY: " << test_binary << std::endl;

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        // skip all benchmarks
        return 0;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    AUTIL_LOG_SHUTDOWN();
}
