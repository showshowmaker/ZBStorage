#include "demo_result.h"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace zb::demo {

namespace {

std::vector<std::string> SplitLines(const std::string& text) {
    std::vector<std::string> lines;
    std::istringstream input(text);
    std::string line;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(line);
    }
    return lines;
}

bool StartsWith(const std::string& value, const std::string& prefix) {
    return value.rfind(prefix, 0) == 0;
}

std::string Trim(const std::string& value) {
    const auto begin = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    });
    const auto end = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    }).base();
    if (begin >= end) {
        return {};
    }
    return std::string(begin, end);
}

void ParseStdout(const std::string& stdout_text,
                 std::vector<DemoMetric>* metrics,
                 std::vector<DemoCheck>* checks) {
    if (!metrics || !checks) {
        return;
    }
    for (const std::string& raw_line : SplitLines(stdout_text)) {
        const std::string line = Trim(raw_line);
        if (line.empty() || StartsWith(line, "====")) {
            continue;
        }
        if (StartsWith(line, "check.")) {
            DemoCheck check;
            const size_t eq = line.find('=');
            if (eq == std::string::npos) {
                continue;
            }
            check.name = line.substr(6, eq - 6);
            const size_t detail_pos = line.find(" detail=\"", eq + 1);
            const std::string status = detail_pos == std::string::npos
                                           ? line.substr(eq + 1)
                                           : line.substr(eq + 1, detail_pos - (eq + 1));
            check.ok = status == "PASS";
            if (detail_pos != std::string::npos) {
                const size_t detail_begin = detail_pos + 9;
                const size_t detail_end = line.size() > 0 && line.back() == '"' ? line.size() - 1 : line.size();
                check.detail = line.substr(detail_begin, detail_end - detail_begin);
            }
            checks->push_back(std::move(check));
            continue;
        }
        const size_t eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        DemoMetric metric;
        metric.key = Trim(line.substr(0, eq));
        metric.value = Trim(line.substr(eq + 1));
        metrics->push_back(std::move(metric));
    }
}

void PrintMetrics(const std::vector<DemoMetric>& metrics) {
    if (metrics.empty()) {
        return;
    }
    std::cout << "\n[关键指标]\n";
    size_t width = 0;
    for (const auto& metric : metrics) {
        width = std::max(width, metric.key.size());
    }
    for (const auto& metric : metrics) {
        std::cout << std::left << std::setw(static_cast<int>(width + 2)) << metric.key
                  << metric.value << '\n';
    }
}

void PrintChecks(const std::vector<DemoCheck>& checks) {
    if (checks.empty()) {
        return;
    }
    std::cout << "\n[校验结果]\n";
    for (const auto& check : checks) {
        std::cout << (check.ok ? "PASS " : "FAIL ") << check.name;
        if (!check.detail.empty()) {
            std::cout << "  " << check.detail;
        }
        std::cout << '\n';
    }
}

void PrintRawBlock(const std::string& title, const std::string& content) {
    if (Trim(content).empty()) {
        return;
    }
    std::cout << "\n[" << title << "]\n";
    std::cout << content;
    if (!content.empty() && content.back() != '\n') {
        std::cout << '\n';
    }
}

} // namespace

ScopedStreamCapture::ScopedStreamCapture()
    : stdout_stream_(new std::ostringstream()),
      stderr_stream_(new std::ostringstream()) {
    cout_buf_ = std::cout.rdbuf(stdout_stream_->rdbuf());
    cerr_buf_ = std::cerr.rdbuf(stderr_stream_->rdbuf());
}

ScopedStreamCapture::~ScopedStreamCapture() {
    if (cout_buf_) {
        std::cout.rdbuf(cout_buf_);
    }
    if (cerr_buf_) {
        std::cerr.rdbuf(cerr_buf_);
    }
    delete stdout_stream_;
    delete stderr_stream_;
}

std::string ScopedStreamCapture::Stdout() const {
    return stdout_stream_ ? stdout_stream_->str() : std::string();
}

std::string ScopedStreamCapture::Stderr() const {
    return stderr_stream_ ? stderr_stream_->str() : std::string();
}

DemoRunResult BuildResultFromOutput(const std::string& title,
                                    const std::string& command,
                                    const std::string& usage,
                                    bool ok,
                                    const std::string& success_summary,
                                    const std::string& failure_summary,
                                    const std::string& stdout_text,
                                    const std::string& stderr_text) {
    DemoRunResult result;
    result.title = title;
    result.command = command;
    result.usage = usage;
    result.ok = ok;
    result.summary = ok ? success_summary : failure_summary;
    result.raw_stdout = stdout_text;
    result.raw_stderr = stderr_text;
    ParseStdout(stdout_text, &result.metrics, &result.checks);
    return result;
}

void RenderResult(const DemoRunResult& result) {
    std::cout << "\n========================================\n";
    std::cout << " " << result.title << '\n';
    std::cout << "========================================\n";
    std::cout << "结果: " << (result.ok ? "PASS" : "FAIL") << '\n';
    if (!result.summary.empty()) {
        std::cout << "摘要: " << result.summary << '\n';
    }
    if (!result.command.empty()) {
        std::cout << "命令: " << result.command << '\n';
    }
    if (!result.usage.empty()) {
        std::cout << "用法: " << result.usage << '\n';
    }

    PrintMetrics(result.metrics);
    PrintChecks(result.checks);
    PrintRawBlock("原始输出", result.raw_stdout);
    PrintRawBlock("错误输出", result.raw_stderr);
}

} // namespace zb::demo
