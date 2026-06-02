#include "Logging.h"

#include "CurrentThread.h"
#include "Timestamp.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sstream>

namespace reactor
{
    __thread char t_errnobuf[512];
    __thread char t_time[64];
    __thread time_t t_lastsecond;

    const char *strerror_tl(int savedErrno)
    {
        return strerror_r(savedErrno, t_errnobuf, sizeof t_errnobuf);
    }

    Logger::LogLevel initLogLevel()
    {
        if (::getenv("REACTOR_LOG_TRACE"))
        {
            return Logger::TRACE;
        }
        if (::getenv("REACTOR_LOG_DEBUG"))
        {
            return Logger::DEBUG;
        }
        else
        {
            return Logger::INFO;
        }
    }

    Logger::LogLevel g_logLevel = initLogLevel();

    const char *LogLevelName[Logger::NUM_LOG_LEVEL] = {
        "[TRACE ]",
        "[DEBUG ]",
        "[INFO  ]",
        "[WARN  ]",
        "[ERROR ]",
        "[FATAL ]"};

    class T
    {
    public:
        T(const char *str, unsigned length)
            : str_(str),
              length_(length)
        {
            assert(strlen(str_) == length_);
        }
        const char *str_;
        unsigned length_;
    };

    inline LogStream &operator<<(LogStream &s, T v)
    {
        s.append(v.str_, v.length_);
        return s;
    }

    inline LogStream &operator<<(LogStream &s, Logger::SourceFile &file)
    {
        s.append(file.data_, file.size_);
        return s;
    }

    void defaultOutput(const char *msg, int len)
    {
        size_t expected = static_cast<size_t>(len);
        size_t n = fwrite(msg, 1, len, stdout);
        if (n != expected)
        {
            int savedErrno = errno;
            fprintf(stderr,
                    "muduo defaultOutput: fwrite wrote %zu of %zu bytes, errno=%d (%s)\n",
                    n,
                    expected,
                    savedErrno,
                    strerror(savedErrno));
        }
    }

    void defaultFlush()
    {
        fflush(stdout);
    }

    Logger::OutputFunc g_output = defaultOutput;
    Logger::FlushFunc g_flush = defaultFlush;

}

using namespace reactor;

Logger::Impl::Impl(LogLevel level, int savedErrno, const SourceFile &file, int line)
    : time_(Timestamp::now()),
      stream_(),
      level_(level),
      line_(line),
      basename_(file)
{
    formatTime();
    CurrentThread::tid();
    stream_ << T(CurrentThread::tidString(), CurrentThread::tidStringLength());
    stream_ << ' ';
    stream_ << T(LogLevelName[level], 8);
    stream_ << ' ';
    if (savedErrno != 0)
    {
        stream_ << strerror_tl(savedErrno) << " (errno ="<< savedErrno << ")";
    }
}

void Logger::Impl::formatTime()
{
    int64_t microSecondsSinceEpoch = time_.microSecondsSinceEpoch();
    time_t seconds = static_cast<time_t>(microSecondsSinceEpoch / Timestamp::kMicroSecondsPerSecond);

    int microseconds = static_cast<int> (microSecondsSinceEpoch % Timestamp::kMicroSecondsPerSecond);

    struct tm tm_time;
    localtime_r(&seconds, &tm_time);

    char buf[100];
    snprintf(buf, sizeof buf, 
             "%4d%02d%02d %02d:%02d:%02d.%06d",
             tm_time.tm_year + 1900,
             tm_time.tm_mon + 1,
             tm_time.tm_mday,
             tm_time.tm_hour,
             tm_time.tm_min,
             tm_time.tm_sec,
             microseconds);
    stream_ << buf;
    stream_ << ' ';
}

void Logger::Impl::finish()
{
    stream_ << " - " << basename_ << ':' << line_ << '\n';
}

Logger::Logger(SourceFile file, int line)
  : impl_(INFO, 0, file, line)
{
}

Logger::Logger(SourceFile file, int line, LogLevel level, const char* func)
  : impl_(level, 0, file, line)
{
  impl_.stream_ << func << ' ';
}

Logger::Logger(SourceFile file, int line, LogLevel level)
  : impl_(level, 0, file, line)
{
}

Logger::Logger(SourceFile file, int line, bool toAbort)
  : impl_(toAbort?FATAL:ERROR, errno, file, line)
{
}

Logger::~Logger()
{
    impl_.finish();
    const LogStream::Buffer& buf(stream().buffer());   //引用
    g_output(buf.data(), buf.length());
    if (impl_.level_ == FATAL)
    {
        g_flush();
        abort();
    }
}

void Logger::setLogLevel(Logger::LogLevel level)
{
  g_logLevel = level;
}

void Logger::setOutput(OutputFunc out)
{
  g_output = out;
}

void Logger::setFlush(FlushFunc flush)
{
  g_flush = flush;
}