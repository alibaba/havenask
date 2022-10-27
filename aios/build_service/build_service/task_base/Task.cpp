#include "build_service/task_base/Task.h"

using namespace std;

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, Task);

Task::Task() {
    setBeeperCollector(WORKER_ERROR_COLLECTOR_NAME);    
}

Task::~Task() {
}

void Task::handleFatalError(const string& message) {
    int64_t ADMIN_HEARTBEAT_INTERVAL = 1 * 1000 * 1000;
    int64_t WORKER_EXEC_HEARTBEAT_INTERVAL = 1 * 1000 * 1000;
    usleep(ADMIN_HEARTBEAT_INTERVAL * 2 +
           WORKER_EXEC_HEARTBEAT_INTERVAL * 2);
    BS_LOG_FLUSH();
    cerr << message << endl;
    // wait current collected by admin, maybe not accurate
    cout << "i will exit" << endl;

    string msg = message + ". i will exit";
    BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, msg);
    BEEPER_CLOSE();
    _exit(-1);
}

}
}
