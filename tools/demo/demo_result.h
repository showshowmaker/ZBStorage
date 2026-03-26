#pragma once

#include <iosfwd>
#include <string>
#include <vector>

namespace zb::demo {

struct DemoMetric {
    std::string key;
    std::string value;
};

struct DemoCheck {
    std::string name;
    bool ok{false};
    std::string detail;
};

struct DemoRunResult {
    std::string title;
    std::string command;
    std::string usage;
    bool ok{false};
    std::string summary;
    std::vector<DemoMetric> metrics;
    std::vector<DemoCheck> checks;
    std::string raw_stdout;
    std::string raw_stderr;
};

class ScopedStreamCapture {
public:
    ScopedStreamCapture();
    ~ScopedStreamCapture();

    std::string Stdout() const;
    std::string Stderr() const;

private:
    std::streambuf* cout_buf_{};
    std::streambuf* cerr_buf_{};
    std::ostringstream* stdout_stream_{};
    std::ostringstream* stderr_stream_{};
};

DemoRunResult BuildResultFromOutput(const std::string& title,
                                    const std::string& command,
                                    const std::string& usage,
                                    bool ok,
                                    const std::string& success_summary,
                                    const std::string& failure_summary,
                                    const std::string& stdout_text,
                                    const std::string& stderr_text);

void RenderResult(const DemoRunResult& result);

} // namespace zb::demo
