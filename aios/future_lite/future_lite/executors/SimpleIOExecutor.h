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
#ifndef FUTURE_SIMPLE_IO_EXECUTOR_H
#define FUTURE_SIMPLE_IO_EXECUTOR_H

#include <thread>
#include <unistd.h>
#include <sys/syscall.h>   /* For SYS_xxx definitions */
#include "future_lite/IOExecutor.h"
#include "autil/Lock.h"
#include <aio.h>
#include <string.h>

namespace future_lite {

namespace executors {

class SimpleIOExecutor : public IOExecutor {
public:
    static constexpr int kMaxAio = 8;

public:
    SimpleIOExecutor() {
        _id = 0;
    }
    virtual ~SimpleIOExecutor() {}

    SimpleIOExecutor(const IOExecutor &) = delete;
    SimpleIOExecutor& operator = (const IOExecutor &) = delete;

private:
    autil::ThreadMutex _mutex;
    std::map<int32_t, std::pair<aiocb*, AIOCallback>> _ongoing;
    std::atomic<int32_t> _id;
    bool _sleepCpuForTest = false;

public:
    bool init() {
        _sleepCpuForTest = getenv("SLEEP_CPU_FOR_TEST");
        _loopThread = std::thread([this]() mutable { this->loop(); });
        return true;
    }

    void destroy() {
        _shutdown = true;
        if (_loopThread.joinable()) {
            _loopThread.join();
        }
        while (!_ongoing.empty()) {
            usleep(500);
        }
    }

    void loop() {
        while (!_shutdown) {
            autil::ScopedLock lock(_mutex);
            for (auto it = _ongoing.begin(); it != _ongoing.end(); ++it) {
                if (aio_error(it->second.first) != EINPROGRESS) {
                    auto ret = aio_return(it->second.first);
                    it->second.second(ret);
                    delete it->second.first;
                    _ongoing.erase(it);
                    break;
                }
            }
            if (_sleepCpuForTest) {
                usleep(100 * 1000);
            }
        }
    }

public:
    void submitIO(int fd, iocb_cmd cmd, void* buffer, size_t length, off_t offset,
                  AIOCallback cbfn) override {
        aiocb* io = new aiocb();
        memset(io, 0, sizeof(aiocb));
        io->aio_fildes = fd;
        io->aio_buf = (char*)buffer;
        io->aio_offset = offset;
        io->aio_nbytes = length;
        int ret = 0;
        if (cmd == IOCB_CMD_PREAD) {
            io->aio_lio_opcode = LIO_READ;
            ret = aio_read(io);
        } else if (cmd == IOCB_CMD_PWRITE) {
            io->aio_lio_opcode = LIO_WRITE;
            ret = aio_write(io);
        } else {
            ret = -1;
        }
        if (ret < 0) {
            delete io;
            cbfn(ret);
        } else {
            autil::ScopedLock lock(_mutex);
            _id++;
            int id = _id;
            _ongoing[id] = make_pair(io, cbfn);
        }
    }
    void submitIOV(int fd, iocb_cmd cmd, const iovec* iov, size_t count, off_t offset,
                   AIOCallback cbfn) override {
        if (cmd == IOCB_CMD_PREADV) {
            cmd = IOCB_CMD_PREAD;
        } else if (cmd == IOCB_CMD_PWRITEV) {
            cmd = IOCB_CMD_PWRITE;
        } else {
            cbfn(-1);
            return;
        }
        std::shared_ptr<int> left(new int(count));
        std::shared_ptr<int> ret(new int(0));
        std::shared_ptr<autil::ThreadMutex> mutex(new autil::ThreadMutex());

        for (size_t i = 0; i < count; ++i) {
            AIOCallback cb = [left, ret, mutex, cbfn = cbfn](int32_t res) mutable {
                autil::ScopedLock lock(*(mutex.get()));
                if (res < 0) {
                    if (*left != -1) {
                        *left = -1;
                        cbfn(res);
                    }
                } else {
                    *ret += res;
                    (*left)--;
                    if (*left == 0) {
                        cbfn(*ret);
                    }
                }
            };
            submitIO(fd, cmd, iov[i].iov_base, iov[i].iov_len, offset, cb);
            offset += iov[i].iov_len;
        }
    }

private:
    volatile bool _shutdown = false;
    std::thread _loopThread;
};

} // namespace executors

} // namespace future_lite

#endif // FUTURE_SIMPLE_IO_EXECUTOR_H
