#include "bx/engine.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "bx/bounded_queue.h"
#include "bx/chunker.h"
#include "bx/dedup_index.h"
#include "bx/hasher.h"
#include "bx/stats.h"

namespace bx {
namespace {

using Clock = std::chrono::steady_clock;

std::uint64_t TotalInputBytes(const std::vector<FileTask>& dataset) {
  std::uint64_t bytes = 0;
  for (const auto& f : dataset) {
    bytes += static_cast<std::uint64_t>(f.size_bytes);
  }
  return bytes;
}

void ProcessFileEndToEnd(const FileTask& file, const Chunker& chunker, const Hasher& hasher,
                         IDedupIndex* index, ThreadLocalStats* stats) {
  const auto t0 = Clock::now();
  std::vector<ChunkDesc> chunks = chunker.Split(file);
  const auto t1 = Clock::now();

  stats->files_processed += 1;
  stats->bytes_processed += static_cast<std::uint64_t>(file.size_bytes);
  stats->chunk_time_ns += static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

  for (const auto& c : chunks) {
    const auto h0 = Clock::now();
    const Fingerprint fp = hasher.HashChunk(c);
    const auto h1 = Clock::now();

    FingerprintChunk fpc;
    fpc.chunk = c;
    fpc.fingerprint = fp;

    const auto i0 = Clock::now();
    const bool unique = index->CheckAndInsert(fpc);
    const auto i1 = Clock::now();

    stats->hash_time_ns += static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(h1 - h0).count());
    stats->index_time_ns += static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(i1 - i0).count());
    stats->chunks_total += 1;
    if (unique) {
      stats->chunks_unique += 1;
      stats->unique_bytes += static_cast<std::uint64_t>(c.length);
    } else {
      stats->chunks_duplicate += 1;
    }
  }
}

RunResult RunV0(const Config& config, const std::vector<FileTask>& dataset) {
  auto index = CreateIndex(config);
  Chunker chunker(config);
  Hasher hasher;

  std::vector<ThreadLocalStats> thread_stats(1);
  const auto begin = Clock::now();
  for (const auto& file : dataset) {
    ProcessFileEndToEnd(file, chunker, hasher, index.get(), &thread_stats[0]);
  }
  const auto end = Clock::now();

  const double elapsed_sec = std::chrono::duration<double>(end - begin).count();
  RunResult result;
  result.shard_hits = index->SnapshotShardHits();
  result.shard_inserts = index->SnapshotShardInserts();
  result.stats = MergeStats(config, thread_stats, TotalInputBytes(dataset), dataset.size(), elapsed_sec);
  return result;
}

RunResult RunV1OrV2(const Config& config, const std::vector<FileTask>& dataset) {
  auto index = CreateIndex(config);
  const std::size_t threads = std::max<std::size_t>(1, config.worker_threads);

  std::vector<ThreadLocalStats> thread_stats(threads);
  std::atomic<std::size_t> next{0};

  const auto begin = Clock::now();
  std::vector<std::thread> workers;
  workers.reserve(threads);

  for (std::size_t tid = 0; tid < threads; ++tid) {
    workers.emplace_back([&, tid] {
      Chunker chunker(config);
      Hasher hasher;
      ThreadLocalStats& local = thread_stats[tid];

      while (true) {
        const std::size_t i = next.fetch_add(1, std::memory_order_relaxed);
        if (i >= dataset.size()) {
          break;
        }
        ProcessFileEndToEnd(dataset[i], chunker, hasher, index.get(), &local);
      }
    });
  }

  for (auto& t : workers) {
    t.join();
  }
  const auto end = Clock::now();

  const double elapsed_sec = std::chrono::duration<double>(end - begin).count();
  RunResult result;
  result.shard_hits = index->SnapshotShardHits();
  result.shard_inserts = index->SnapshotShardInserts();
  result.stats = MergeStats(config, thread_stats, TotalInputBytes(dataset), dataset.size(), elapsed_sec);
  return result;
}

RunResult RunV3(const Config& config, const std::vector<FileTask>& dataset) {
  auto index = CreateIndex(config);

  BoundedBlockingQueue<FileTask> q_file(config.queue_capacity);
  BoundedBlockingQueue<ChunkBatch> q_chunk(config.queue_capacity);
  BoundedBlockingQueue<FingerprintBatch> q_fp(config.queue_capacity);

  std::vector<ThreadLocalStats> reader_stats(config.reader_threads);
  std::vector<ThreadLocalStats> chunker_stats(config.chunker_threads);
  std::vector<ThreadLocalStats> hasher_stats(config.hasher_threads);
  std::vector<ThreadLocalStats> indexer_stats(config.indexer_threads);

  std::atomic<std::size_t> batch_seq{0};
  const auto begin = Clock::now();

  std::atomic<std::size_t> next_read{0};
  std::vector<std::thread> readers;
  readers.reserve(config.reader_threads);
  for (std::size_t tid = 0; tid < config.reader_threads; ++tid) {
    readers.emplace_back([&, tid] {
      ThreadLocalStats& local = reader_stats[tid];
      while (true) {
        const std::size_t i = next_read.fetch_add(1, std::memory_order_relaxed);
        if (i >= dataset.size()) {
          break;
        }
        FileTask copy = dataset[i];
        q_file.Push(std::move(copy), &local.push_wait_ns);
      }
    });
  }

  std::vector<std::thread> chunkers;
  chunkers.reserve(config.chunker_threads);
  for (std::size_t tid = 0; tid < config.chunker_threads; ++tid) {
    chunkers.emplace_back([&, tid] {
      Chunker chunker(config);
      ThreadLocalStats& local = chunker_stats[tid];

      FileTask file;
      while (q_file.Pop(&file, &local.pop_wait_ns)) {
        local.files_processed += 1;
        local.bytes_processed += static_cast<std::uint64_t>(file.size_bytes);

        const auto t0 = Clock::now();
        std::vector<ChunkDesc> chunks = chunker.Split(file);
        const auto t1 = Clock::now();
        local.chunk_time_ns += static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());

        std::size_t off = 0;
        while (off < chunks.size()) {
          const std::size_t n = std::min<std::size_t>(config.batch_size, chunks.size() - off);
          ChunkBatch batch;
          batch.batch_id = batch_seq.fetch_add(1, std::memory_order_relaxed);
          batch.chunks.reserve(n);
          for (std::size_t i = 0; i < n; ++i) {
            batch.chunks.push_back(std::move(chunks[off + i]));
          }
          off += n;
          q_chunk.Push(std::move(batch), &local.push_wait_ns);
        }
      }
    });
  }

  std::vector<std::thread> hashers;
  hashers.reserve(config.hasher_threads);
  for (std::size_t tid = 0; tid < config.hasher_threads; ++tid) {
    hashers.emplace_back([&, tid] {
      Hasher hasher;
      ThreadLocalStats& local = hasher_stats[tid];

      ChunkBatch in;
      while (q_chunk.Pop(&in, &local.pop_wait_ns)) {
        FingerprintBatch out;
        out.batch_id = in.batch_id;
        out.chunks.reserve(in.chunks.size());

        for (auto& c : in.chunks) {
          const auto h0 = Clock::now();
          const Fingerprint fp = hasher.HashChunk(c);
          const auto h1 = Clock::now();
          local.hash_time_ns += static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::nanoseconds>(h1 - h0).count());

          FingerprintChunk fc;
          fc.chunk = std::move(c);
          fc.fingerprint = fp;
          out.chunks.push_back(std::move(fc));
        }

        q_fp.Push(std::move(out), &local.push_wait_ns);
      }
    });
  }

  std::vector<std::thread> indexers;
  indexers.reserve(config.indexer_threads);
  for (std::size_t tid = 0; tid < config.indexer_threads; ++tid) {
    indexers.emplace_back([&, tid] {
      ThreadLocalStats& local = indexer_stats[tid];

      FingerprintBatch batch;
      while (q_fp.Pop(&batch, &local.pop_wait_ns)) {
        for (const auto& fc : batch.chunks) {
          const auto i0 = Clock::now();
          const bool unique = index->CheckAndInsert(fc);
          const auto i1 = Clock::now();
          local.index_time_ns += static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::nanoseconds>(i1 - i0).count());

          local.chunks_total += 1;
          if (unique) {
            local.chunks_unique += 1;
            local.unique_bytes += static_cast<std::uint64_t>(fc.chunk.length);
          } else {
            local.chunks_duplicate += 1;
          }
        }
      }
    });
  }

  for (auto& t : readers) {
    t.join();
  }
  q_file.Close();
  for (auto& t : chunkers) {
    t.join();
  }
  q_chunk.Close();

  for (auto& t : hashers) {
    t.join();
  }
  q_fp.Close();

  for (auto& t : indexers) {
    t.join();
  }

  const auto end = Clock::now();
  const double elapsed_sec = std::chrono::duration<double>(end - begin).count();

  std::vector<ThreadLocalStats> merged;
  merged.reserve(reader_stats.size() + chunker_stats.size() + hasher_stats.size() + indexer_stats.size());
  merged.insert(merged.end(), reader_stats.begin(), reader_stats.end());
  merged.insert(merged.end(), chunker_stats.begin(), chunker_stats.end());
  merged.insert(merged.end(), hasher_stats.begin(), hasher_stats.end());
  merged.insert(merged.end(), indexer_stats.begin(), indexer_stats.end());

  RunResult result;
  result.stats = MergeStats(config, merged, TotalInputBytes(dataset), dataset.size(), elapsed_sec);
  result.shard_hits = index->SnapshotShardHits();
  result.shard_inserts = index->SnapshotShardInserts();
  result.queue_peaks = {q_file.PeakSize(), q_chunk.PeakSize(), q_fp.PeakSize()};
  return result;
}

}  // namespace

RunResult ExecuteOnce(const Config& config, const std::vector<FileTask>& dataset) {
  if (config.version == Version::kV0) {
    return RunV0(config, dataset);
  }
  if (config.version == Version::kV1 || config.version == Version::kV2) {
    return RunV1OrV2(config, dataset);
  }
  return RunV3(config, dataset);
}

}  // namespace bx
