const fs = require("fs");
const path = require("path");

module.exports = () => {
    let configFilePath = path.resolve(__dirname, "..", "DefaultConfig.json");
    return JSON.parse(fs.readFileSync(configFilePath, "utf8"));
};
