#include <benchmark/benchmark.h>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string>

#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftReaderImpl.h"
#include "swift/client/fake_client/FakeClientHelper.h"
#include "swift/client/fake_client/FakeSwiftAdminAdapter.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace swift::protocol;
using namespace swift::common;
using namespace swift::network;

namespace swift {
namespace client {

class ReaderBenchmark : public benchmark::Fixture {
public:
    ReaderBenchmark() {}
    virtual ~ReaderBenchmark() {}
    SwiftReaderImplPtr createReader(uint32_t partitionCount);
};
SwiftReaderImplPtr ReaderBenchmark::createReader(uint32_t partitionCount) {
    FakeSwiftAdminAdapter *fakeAdapter = new FakeSwiftAdminAdapter;
    fakeAdapter->setPartCount(partitionCount);
    SwiftAdminAdapterPtr adapter(fakeAdapter);
    SwiftReaderConfig config;
    config.topicName = "test";
    SwiftReaderImplPtr reader(FakeClientHelper::createSimpleReader(adapter, config, partitionCount, 0));
    return reader;
}

BENCHMARK_F(ReaderBenchmark, TestGetProgress1Part)(benchmark::State &state) {
    SwiftReaderImplPtr reader = createReader(1);
    for (auto _ : state) {
        ReaderProgress progress;
        reader->getReaderProgress(progress);
    }
}
BENCHMARK_F(ReaderBenchmark, TestGetProgress4Part)(benchmark::State &state) {
    SwiftReaderImplPtr reader = createReader(4);
    for (auto _ : state) {
        ReaderProgress progress;
        reader->getReaderProgress(progress);
    }
}
BENCHMARK_F(ReaderBenchmark, TestGetProgress16Part)(benchmark::State &state) {
    SwiftReaderImplPtr reader = createReader(16);
    for (auto _ : state) {
        ReaderProgress progress;
        reader->getReaderProgress(progress);
    }
}

BENCHMARK_F(ReaderBenchmark, TestGetProgress64Part)(benchmark::State &state) {
    SwiftReaderImplPtr reader = createReader(64);
    for (auto _ : state) {
        ReaderProgress progress;
        reader->getReaderProgress(progress);
    }
}

BENCHMARK_F(ReaderBenchmark, TestGetProgress128Part)(benchmark::State &state) {
    SwiftReaderImplPtr reader = createReader(128);
    for (auto _ : state) {
        ReaderProgress progress;
        reader->getReaderProgress(progress);
    }
}

BENCHMARK_F(ReaderBenchmark, TestGetProgress256Part)(benchmark::State &state) {
    SwiftReaderImplPtr reader = createReader(256);
    for (auto _ : state) {
        ReaderProgress progress;
        reader->getReaderProgress(progress);
    }
}

BENCHMARK_F(ReaderBenchmark, TestGetProgress512Part)(benchmark::State &state) {
    SwiftReaderImplPtr reader = createReader(512);
    for (auto _ : state) {
        ReaderProgress progress;
        reader->getReaderProgress(progress);
    }
}

}; // namespace client
}; // namespace swift
