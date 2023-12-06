// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "io/fs/local_file_writer.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fcntl.h>
#include <glog/logging.h>
#include <limits.h>
#include <stdint.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <ostream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/cached_local_file_writer.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "util/doris_metrics.h"

namespace doris {
namespace detail {

Status sync_dir(const io::Path& dirname) {
    int fd;
    RETRY_ON_EINTR(fd, ::open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
    if (-1 == fd) {
        return Status::IOError("cannot open {}: {}", dirname.native(), std::strerror(errno));
    }
#ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) < 0) {
        return Status::IOError("cannot sync {}: {}", dirname.native(), std::strerror(errno));
    }
#else
    if (0 != ::fdatasync(fd)) {
        return Status::IOError("cannot fdatasync {}: {}", dirname.native(), std::strerror(errno));
    }
#endif
    ::close(fd);
    return Status::OK();
}

} // namespace detail

namespace io {

CachedLocalFileWriter::CachedLocalFileWriter(Path path, int fd, FileSystemSPtr fs)
        : FileWriter(std::move(path), fs), _fd(fd) {
    _opened = true;
    DorisMetrics::instance()->local_file_open_writing->increment(1);
    DorisMetrics::instance()->local_file_writer_total->increment(1);
}

CachedLocalFileWriter::CachedLocalFileWriter(Path path, int fd)
        : CachedLocalFileWriter(path, fd, global_local_filesystem()) {}

CachedLocalFileWriter::~CachedLocalFileWriter() {
    if (_opened) {
        close();
    }
    CHECK(!_opened || _closed) << "open: " << _opened << ", closed: " << _closed;
}

Status CachedLocalFileWriter::close() {
    return _close(true);
}

Status CachedLocalFileWriter::abort() {
    RETURN_IF_ERROR(_close(false));
    return io::global_local_filesystem()->delete_file(_path);
}

Status CachedLocalFileWriter::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    _dirty = true;

    _data_cnt += data_cnt;
    _page.emplace_back(std::pair(data, data_cnt));
    return Status::OK();
}

Status CachedLocalFileWriter::_flush_all() {
    DCHECK(!_closed);
    _dirty = true;

    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;

    std::vector<iovec> iov;

    for (const auto& [fst, snd] : _page) {
        for (size_t i = 0; i < snd; i++) {
            const Slice& result = fst[i];
            bytes_req += result.size;
        }

        // Merge slices into a single iovec entry
        iovec merged_iov = _merge_slices(fst, snd);
        iov.push_back(merged_iov);
    }

    ssize_t res;
    RETRY_ON_EINTR(res, ::writev(_fd, iov.data(), iov.size()));
    if (UNLIKELY(res < 0)) {
        LOG(INFO) << "can not write to " << _path.native() << ", error no: " << errno
                  << ", error msg: " << strerror(errno);
        perror("writev");
        return Status::IOError("cannot write to {}: {}", _path.native(), std::strerror(errno));
    }

    _bytes_appended += bytes_req;
    return Status::OK();
}

iovec CachedLocalFileWriter::_merge_slices(const Slice* slices, size_t count) {
    size_t total_size = 0;
    for (size_t i = 0; i < count; i++) {
        total_size += slices[i].size;
    }

    const std::unique_ptr<char[]> merged_data(new char[total_size]);

    size_t offset = 0;
    for (size_t i = 0; i < count; i++) {
        std::memcpy(merged_data.get() + offset, slices[i].data, slices[i].size);
        offset += slices[i].size;
    }

    return {merged_data.get(), total_size};
}

Status CachedLocalFileWriter::write_at(size_t offset, const Slice& data) {
    DCHECK(!_closed);
    _dirty = true;

    size_t bytes_req = data.size;
    char* from = data.data;

    while (bytes_req != 0) {
        auto res = ::pwrite(_fd, from, bytes_req, offset);
        if (-1 == res && errno != EINTR) {
            return Status::IOError("cannot write to {}: {}", _path.native(), std::strerror(errno));
        }
        if (res > 0) {
            from += res;
            bytes_req -= res;
        }
    }
    return Status::OK();
}

Status CachedLocalFileWriter::finalize() {
    DCHECK(!_closed);
    if (_dirty) {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return Status::IOError("cannot sync {}: {}", _path.native(), std::strerror(errno));
        }
#endif
    }
    _flush_all();
    return Status::OK();
}

Status CachedLocalFileWriter::_close(bool sync) {
    if (_closed) {
        return Status::OK();
    }

    _flush_all();

    _closed = true;
    if (sync && _dirty) {
#ifdef __APPLE__
        if (fcntl(_fd, F_FULLFSYNC) < 0) {
            return Status::IOError("cannot sync {}: {}", _path.native(), std::strerror(errno));
        }
#else
        if (0 != ::fdatasync(_fd)) {
            return Status::IOError("cannot fdatasync {}: {}", _path.native(), std::strerror(errno));
        }
#endif
        RETURN_IF_ERROR(detail::sync_dir(_path.parent_path()));
        _dirty = false;
    }

    DorisMetrics::instance()->local_file_open_writing->increment(-1);
    DorisMetrics::instance()->file_created_total->increment(1);
    DorisMetrics::instance()->local_bytes_written_total->increment(_bytes_appended);

    if (0 != ::close(_fd)) {
        return Status::IOError("cannot close {}: {}", _path.native(), std::strerror(errno));
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
