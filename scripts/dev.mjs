import { spawn } from "child_process";

const isWindows = process.platform === "win32";
const npmCmd = isWindows ? "npm.cmd" : "npm";
let shuttingDown = false;
const processes = [];
const backendHealthUrl = "http://localhost:3001/api/health";

function npmSpawnArgs(scriptName) {
  const npmArgs = ["run", scriptName];
  if (!isWindows) {
    return {
      command: npmCmd,
      args: npmArgs
    };
  }
  return {
    command: process.env.ComSpec || "cmd.exe",
    args: ["/d", "/s", "/c", npmCmd, ...npmArgs]
  };
}

function startScript(label, scriptName) {
  const { command, args } = npmSpawnArgs(scriptName);
  const child = spawn(command, args, {
    stdio: "inherit",
    shell: false,
    windowsHide: false
  });
  child.on("error", (error) => {
    console.error(`[dev:${label}] failed to start: ${error?.message || error}`);
    if (!shuttingDown) {
      shuttingDown = true;
      for (const proc of processes) {
        if (proc !== child && !proc.killed) proc.kill();
      }
    }
    process.exit(1);
  });
  child.on("exit", (code, signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    for (const proc of processes) {
      if (proc !== child && !proc.killed) proc.kill();
    }
    process.exit(code ?? (signal ? 1 : 0));
  });
  processes.push(child);
}

async function backendAlreadyRunning() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 700);
  try {
    const response = await fetch(backendHealthUrl, {
      signal: controller.signal
    });
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

if (await backendAlreadyRunning()) {
  console.log("[dev] Backend API already running on http://localhost:3001");
} else {
  startScript("backend", "dev:backend");
}
startScript("frontend", "dev:frontend");

function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const proc of processes) {
    if (!proc.killed) proc.kill();
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
