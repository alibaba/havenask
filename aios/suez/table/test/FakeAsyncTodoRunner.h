#pragma once

#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "suez/table/Todo.h"
#include "suez/table/TodoRunner.h"

namespace suez {

// FakeAsyncTodoRunner 可以手动控制异步执行何时结束,用于模拟dp等操作
class FakeAsyncTodoRunner final : public TodoRunner {
public:
    FakeAsyncTodoRunner(autil::ThreadPool *threadPool, const std::string &name);
    ~FakeAsyncTodoRunner();

    bool run(const std::shared_ptr<Todo> &todo);
    bool finishAsyncTask(PartitionId &pid);
    void stopAll();

private:
    bool isOngoing(const std::string &identifier) const;
    bool markOngoing(const std::string &identifier);
    void remove(const std::string &identifier);
    bool checkStart(const std::string &identifier);
    void setStart(const std::string &identifier);
    bool checkFinished(const std::string &identifier);
    void setFinished(const std::string &identifier);
    bool updateStatus(Todo *todo);

private:
    autil::ThreadPool *_threadPool;
    mutable autil::ThreadMutex _mutex;
    std::atomic<bool> _stopRunner;
    std::unordered_map<std::string, std::atomic<bool>> _ongoingFlagMap;
    std::unordered_map<std::string, std::atomic<bool>> _finishFlagMap;
};

} // namespace suez
