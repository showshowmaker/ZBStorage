#pragma once

#include <string>

namespace zb::scheduler {

struct ActuatorResult {
    bool success{false};
    std::string message;
};

class NodeActuator {
public:
    virtual ~NodeActuator() = default;

    virtual ActuatorResult StartNode(const std::string& node_id, const std::string& address) = 0;
    virtual ActuatorResult StopNode(const std::string& node_id, const std::string& address, bool force) = 0;
    virtual ActuatorResult RebootNode(const std::string& node_id, const std::string& address) = 0;
};

class ShellNodeActuator : public NodeActuator {
public:
    ShellNodeActuator(std::string start_template,
                      std::string stop_template,
                      std::string reboot_template);

    ActuatorResult StartNode(const std::string& node_id, const std::string& address) override;
    ActuatorResult StopNode(const std::string& node_id, const std::string& address, bool force) override;
    ActuatorResult RebootNode(const std::string& node_id, const std::string& address) override;

private:
    ActuatorResult ExecuteTemplate(const std::string& command_template,
                                   const std::string& node_id,
                                   const std::string& address,
                                   bool force);

    std::string start_template_;
    std::string stop_template_;
    std::string reboot_template_;
};

} // namespace zb::scheduler
