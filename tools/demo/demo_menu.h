#pragma once

#include <string>
#include <vector>

namespace zb::demo {

struct MenuActionSpec {
    std::string id;
    std::string title;
    std::string description;
    std::string usage;
    std::vector<std::string> aliases;
};

void RenderMenu(const std::string& title, const std::vector<MenuActionSpec>& actions);
const MenuActionSpec* FindAction(const std::vector<MenuActionSpec>& actions, const std::string& token);

} // namespace zb::demo
