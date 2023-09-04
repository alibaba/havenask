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
#pragma once

#include "future_lite/Executor.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"

namespace navi_ops {

static const std::string FuncDataType = "FunctionData";

class FunctionData : public navi::Data {
public:
    FunctionData(future_lite::Executor::Func func) : Data(FuncDataType), _func(func), _finish(false) {}
    ~FunctionData() {
        if (!_finish) {
            run();
        }
    }
    void run() {
        _func();
        _finish = true;
    }

private:
    future_lite::Executor::Func _func;
    bool _finish;
};

class NaviAsyncPipeExecutor : public future_lite::Executor,
                              public std::enable_shared_from_this<NaviAsyncPipeExecutor>
{
public:
    NaviAsyncPipeExecutor(navi::AsyncPipePtr asyncPipe) : _asyncPipe(asyncPipe) {}
    ~NaviAsyncPipeExecutor() {}
    bool schedule(future_lite::Executor::Func func) override {
        // IMPORTANT:
        // func() may release ref to current executor, keep it during `schedule`
        auto holder = shared_from_this();
        navi::DataPtr data(new FunctionData(std::move(func)));
        auto ec = _asyncPipe->setData(data);
        if (ec != navi::EC_NONE) {
            // TODO(xiaohao.yxh) :maybe need a global queue to process this work item
            data.reset();
        }
        ++_scheduleCount;
        return true;
    }
    bool currentThreadInExecutor() const override {
        assert(false);
        return false;
    }
    future_lite::ExecutorStat stat() const override {
        assert(false);
        return {};
    }
    future_lite::IOExecutor *getIOExecutor() override {
        assert(false);
        return nullptr;
    }

private:
    navi::AsyncPipePtr _asyncPipe;
    size_t _scheduleCount = 0;
};

} // namespace navi_ops
